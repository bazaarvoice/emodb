package com.bazaarvoice.emodb.common.stash;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Closer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Iterable access to StashRowIterators which can be closed and ensures that any iterators it produces are also closed.
 */
abstract public class StashRowIterable implements Iterable<Map<String, Object>>, Closeable {

    private final List<StashRowIterator> _openIterators = Lists.newArrayList();
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
                Closeables.close(_initialIterator, true);
            } catch (IOException e2) {
                // Already caught and logged
            }
            throw Throwables.propagate(e);
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
                // Use a closer to cleanly close all iterators even if one throws an exception on close
                Closer closer = Closer.create();
                for (StashRowIterator iterator : _openIterators) {
                    closer.register(iterator);
                }
                closer.close();
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
