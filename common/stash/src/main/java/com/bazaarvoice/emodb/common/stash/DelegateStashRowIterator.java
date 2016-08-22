package com.bazaarvoice.emodb.common.stash;

import java.io.IOException;
import java.util.Map;

/**
 * StashRowIterator which delegates calls to another instance.  This class can be extended to customize the activity
 * on each method call.
 */
public class DelegateStashRowIterator implements StashRowIterator {

    private final StashRowIterator _delegate;

    public DelegateStashRowIterator(StashRowIterator delegate) {
        _delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        return _delegate.hasNext();
    }

    @Override
    public Map<String, Object> next() {
        return _delegate.next();
    }

    @Override
    public void remove() {
        _delegate.remove();
    }

    @Override
    public void close() throws IOException {
        _delegate.close();
    }
}
