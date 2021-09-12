package com.bazaarvoice.emodb.common.api.impl;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Similar to {@link com.google.common.collect.Iterators#partition(java.util.Iterator, int)} except that it
 * adaptively adjusts the size of each partition in an attempt to keep the time between each loop of the
 * partition at a particular value.
 */
public class TimePartitioningIterator<T> extends AbstractIterator<List<T>> {

    private final Iterator<T> _iterator;
    private final int _minSize;
    private final int _maxSize;
    private final long _goalMillis;
    private int _batchSize;
    private long _timestamp;

    /**
     * Partitions the specified iterable, adjusting the size of each partition with the goal of making each
     * iteration take time equal to the {@code iterationGoalMillis}.  This works best when the amount of
     * work done between each call to {@link #hasNext()} is linear with respect to the size of the partition.
     */
    public static <T> Iterable<List<T>> partition(
            final Iterable<T> iterable, final int initialSize, final int minSize, final int maxSize,
            final Duration iterationGoal) {
        return new Iterable<List<T>>() {
            @Override
            public Iterator<List<T>> iterator() {
                return new TimePartitioningIterator<>(
                        iterable.iterator(), initialSize, minSize, maxSize, iterationGoal);
            }
        };
    }

    /**
     * Partitions the specified iterator, adjusting the size of each partition with the goal of making each
     * iteration take time equal to the {@code iterationGoalMillis}.  This works best when the amount of
     * work done between each call to {@link #hasNext()} is linear with respect to the size of the partition.
     */
    public TimePartitioningIterator(Iterator<T> iterator, int initialSize, int minSize, int maxSize, Duration iterationGoal) {
        checkArgument(iterationGoal.compareTo(Duration.ZERO) > 0);
        _iterator = checkNotNull(iterator);
        checkArgument(minSize > 0 && minSize <= maxSize);
        _minSize = minSize;
        _maxSize = maxSize;
        _goalMillis = iterationGoal.toMillis();
        _batchSize = initialSize;
    }

    @Override
    protected List<T> computeNext() {
        long now = System.currentTimeMillis();
        if (_timestamp != 0) {
            adjustBatchSize(now - _timestamp);
        }
        _timestamp = now;

        if (!_iterator.hasNext()) {
            return endOfData();
        }

        List<T> list = Lists.newArrayListWithCapacity(_batchSize);
        while (_iterator.hasNext() && list.size() < _batchSize) {
            list.add(_iterator.next());
        }
        return list;
    }

    private void adjustBatchSize(long elapsedMillis) {
        // Try to hit the target goal but don't grow more than double in any one iteration to avoid overshooting.
        double adjustment;
        if (elapsedMillis > 0) {
            adjustment = Math.min(_goalMillis / (double) elapsedMillis, 2.0);
        } else {
            adjustment = 2.0;
        }
        _batchSize = constrain(_minSize, (int) (_batchSize * adjustment), _maxSize);
    }

    private int constrain(int min, int value, int max) {
        return Math.min(Math.max(min, value), max);
    }
}
