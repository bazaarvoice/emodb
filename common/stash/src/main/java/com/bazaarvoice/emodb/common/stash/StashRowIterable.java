package com.bazaarvoice.emodb.common.stash;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Iterable access to StashRowIterators which can be closed and ensures that any iterators it produces are also closed.
 */
abstract public class StashRowIterable implements Iterable<Map<String, Object>>, Closeable {

    private final List<StashRowIterator> _openIterators = new ArrayList<>();
    private StashRowIterator _initialIterator;

    abstract protected StashRowIterator createStashRowIterator();

    /**
     * In order to fail fast create the initial iterator on instantiation.  This way exceptions such as
     * TableNotStashedException will be thrown immediately and not deferred until the first iteration.
     */
    public StashRowIterable() {
        _initialIterator = createStashRowIterator();
        // Force the first record to be evaluated by calling hasNext()
        try {
            _initialIterator.hasNext();
            _openIterators.add(_initialIterator);
        } catch (Exception e) {
            try {
                _initialIterator.close();
            } catch (IOException e2) {
                // ignore
            }
            throw e;
        }
    }

    @Override
    public StashRowIterator iterator() {
        final StashRowIterator iterator;
        if (_initialIterator != null) {
            iterator = _initialIterator;
            _initialIterator = null;
        } else {
            iterator = createStashRowIterator();
            _openIterators.add(iterator);
        }

        // Override the close method to also remove the iterator from the open iterator list
        return new DelegateStashRowIterator(iterator) {
            @Override
            public void close() throws IOException {
                super.close();
                _openIterators.remove(iterator);
            }
        };
    }

    @Override
    public void close() throws IOException {
        if (!_openIterators.isEmpty()) {
            try {
                Exception closeException = null;
                for (StashRowIterator iterator : _openIterators) {
                    try {
                        iterator.close();
                    } catch (Exception e) {
                        // Only maintain the first exception generated
                        if (closeException == null) {
                            closeException = e;
                        }
                    }
                }
                if (closeException != null) {
                    throw (closeException instanceof IOException ? (IOException) closeException : new IOException(closeException));
                }
            } finally {
                _openIterators.clear();
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }
}
