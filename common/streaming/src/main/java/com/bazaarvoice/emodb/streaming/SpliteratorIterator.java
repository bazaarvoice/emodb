package com.bazaarvoice.emodb.streaming;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.stream.StreamSupport;

abstract public class SpliteratorIterator<T> implements Iterator<T> {

    private Iterator<T> _delegate;

    private Iterator<T> delegate() {
        if (_delegate == null) {
            _delegate = StreamSupport.stream(getSpliterator(), false).iterator();
        }
        return _delegate;
    }

    @Override
    public boolean hasNext() {
        return delegate().hasNext();
    }

    @Override
    public T next() {
        return delegate().next();
    }

    @Override
    public void remove() {
        delegate().remove();
    }

    abstract protected Spliterator<T> getSpliterator();
}
