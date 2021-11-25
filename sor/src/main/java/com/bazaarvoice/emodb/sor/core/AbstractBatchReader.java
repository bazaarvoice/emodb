package com.bazaarvoice.emodb.sor.core;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Base class for streaming data in batches from a resource and presenting it as a single iterator.  This differs
 * from other implementations such as {@link Iterators#concat(java.util.Iterator[])})} in that it increases the
 * size of each batch read until a maximum batch size is achieved.  If on any iteration the next batch times out
 * before it can be read it will decrease the batch size and slowly grow it back while attempting to prevent
 * future timeouts.
 */
abstract public class AbstractBatchReader<T> extends AbstractIterator<T> {

    // Minimum number of rows to read in a single batch
    private final int _minBatchSize;
    // Maximum number of rows to read in a single batch
    private final int _maxBatchSize;
    // Amount to increment the next batch size until the maximum is achieved
    private final int _batchIncrementSize;
    private int _currentBatchSize;
    private final Stopwatch _timer = Stopwatch.createUnstarted();
    private Iterator<T> _batch = Collections.emptyIterator();
    // In the event of a timeout record the amount of time that elapsed before the timeout
    private long _lastTimeoutTime;

    /**
     * Returns true if there is potentially more data in the next batch, false if all data has been returned.
     */
    abstract protected boolean hasNextBatch();

    /**
     * Returns an iterator for the next batch, which should target returning "batchSize" rows.
     */
    abstract protected Iterator<T> nextBatch(int batchSize) throws Exception;

    /**
     * If {@link #nextBatch(int)} returned an exception this method determines if it was due to a timeout and
     * therefore can be retried with a smaller batch size.
     */
    abstract protected boolean isTimeoutException(Exception e);

    /**
     * If {@link #nextBatch(int)} returned an exception this method determines if it was due to the amount of
     * returned data and therefore can be retried with a smaller batch size.
     */
    abstract protected boolean isDataSizeException(Exception e);

    protected AbstractBatchReader(int minBatchSize, int initialBatchSize, int maxBatchSize, int batchIncrementSize) {
        checkArgument(minBatchSize > 0, "minBatchSize <= 0");
        checkArgument(minBatchSize <= initialBatchSize, "minBatchSize > initialBatchSize");
        checkArgument(initialBatchSize <= maxBatchSize, "initialBatchSize > maxBatchSize");
        checkArgument(batchIncrementSize > 0, "batchIncrementSize <= 0");

        _minBatchSize = minBatchSize;
        _currentBatchSize = initialBatchSize;
        _maxBatchSize = maxBatchSize;
        _batchIncrementSize = batchIncrementSize;
    }

    @Override
    protected T computeNext() {
        // If the current batch is non-empty return the next row now.
        if (_batch.hasNext()) {
            return _batch.next();
        }

        // Load batches until a non-empty batch is found or all batches have been read.
        boolean nextBatchAvailable;
        do {
            nextBatchAvailable = loadNextBatch();
        } while (!nextBatchAvailable);

        // An empty batch at this point means that all batches have been read and there is no more data.
        if (!_batch.hasNext()) {
            return endOfData();
        }

        return _batch.next();
    }

    /**
     * Loads the next batch from the underlying resource.
     *
     * @return True if a non-empty batch was read or all data has already been read, false if the batch was
     * empty or the batch read timed out.
     */
    private boolean loadNextBatch() {
        if (!hasNextBatch()) {
            // No batches remaining to load
            _batch = Collections.emptyIterator();
            return true;
        }

        Exception batchFetchException = null;

        _timer.start();
        try {
            _batch = nextBatch(_currentBatchSize);
        } catch (Exception e) {
            batchFetchException = e;
        } finally {
            _timer.stop();
        }

        long batchFetchTime = _timer.elapsed(TimeUnit.MILLISECONDS);
        _timer.reset();

        if (batchFetchException != null) {
            boolean isTimeout = isTimeoutException(batchFetchException);
            boolean isDataSize = !isTimeout && isDataSizeException(batchFetchException);

            // Check if the exception was not due to a timeout or size limit, or if the batch size is already at the minimum
            if (!(isTimeout || isDataSize) || _currentBatchSize == _minBatchSize) {
                throw Throwables.propagate(batchFetchException);
            }

            // Reduce the current batch size by half, but not below the minimum size
            _currentBatchSize = Math.max(_currentBatchSize / 2, _minBatchSize);

            if (isTimeout) {
                // Record how long it took to timeout; we'll use this to approximate and try to avoid future timeouts
                _lastTimeoutTime = batchFetchTime;
            }

            return false;
        }

        // We didn't time out.  Evaluate whether to increase the current batch size.

        int nextBatchSize = Math.min(_currentBatchSize + _batchIncrementSize, _maxBatchSize);
        if (nextBatchSize != _currentBatchSize) {
            if (_lastTimeoutTime != 0) {
                // We've seen a timeout; use this as a guide to predict whether the next batch will timeout.
                float msPerRow = (float) batchFetchTime / _currentBatchSize;
                int estimatedMaxBatchSizeUntilTimeout = (int) (_lastTimeoutTime / msPerRow);
                // Check for negative value in case of integer size overrun
                if (estimatedMaxBatchSizeUntilTimeout > 0 && estimatedMaxBatchSizeUntilTimeout < nextBatchSize) {
                    nextBatchSize = estimatedMaxBatchSizeUntilTimeout;
                }
            }

            _currentBatchSize = nextBatchSize;
        }

        // It's possible the underlying resource read the requested number of rows but, after filtering is applied,
        // all rows are excluded.  This can happen, for example, if Cassandra returns the requested number of rows
        // but they are all range ghosts.  In this case return false to prompt the next batch to load immediately.
        return _batch.hasNext();
    }
}
