package com.bazaarvoice.emodb.common.stash;

import com.amazonaws.services.s3.AmazonS3;
import com.bazaarvoice.emodb.streaming.AbstractSpliterator;
import com.bazaarvoice.emodb.streaming.SpliteratorIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;

/**
 * Closeable iterator for Stash scans.
 */
class StashScanIterator extends SpliteratorIterator<Map<String, Object>> implements StashRowIterator {
    private final AmazonS3 _s3;
    private final String _bucket;
    private final String _rootPath;
    private final Iterator<StashSplit> _splits;
    private StashRowIterator _currentIterator;

    StashScanIterator(AmazonS3 s3, String bucket, String rootPath, Iterable<StashSplit> splits) {
        _s3 = s3;
        _bucket = bucket;
        _rootPath = rootPath;
        _splits = splits.iterator();

        moveToNextSplit();
    }

    private void moveToNextSplit() {
        // Close the current split if it is open first
        closeCurrentSplit();

        if (_splits.hasNext()) {
            StashSplit split = _splits.next();
            String key = String.format("%s/%s", _rootPath, split.getKey());
            _currentIterator = new StashSplitIterator(_s3, _bucket, key);
        } else {
            _currentIterator = null;
        }
    }

    private void closeCurrentSplit() {
        if (_currentIterator != null) {
            try {
                _currentIterator.close();
            } catch (IOException ignore) {
                // ignored
            }
            _currentIterator = null;
        }
    }

    @Override
    protected Spliterator<Map<String, Object>> getSpliterator() {
        return new AbstractSpliterator<Map<String, Object>>() {
            @Override
            protected Map<String, Object> computeNext() {
                while (_currentIterator != null) {
                    // Return the next row from the current split if available
                    if (_currentIterator.hasNext()) {
                        return _currentIterator.next();
                    }

                    // Move the the next split and try again
                    moveToNextSplit();
                }

                return endOfStream();
            }
        };
    }

    @Override
    public void close()
            throws IOException {
        closeCurrentSplit();
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }
}
