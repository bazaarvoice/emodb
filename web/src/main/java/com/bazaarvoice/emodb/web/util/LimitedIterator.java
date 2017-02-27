package com.bazaarvoice.emodb.web.util;

import com.google.common.collect.AbstractIterator;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

public class LimitedIterator<T> extends AbstractIterator<T> {
    private final Iterator<T> delegate;
    private final Long limit;

    private final AtomicLong count = new AtomicLong(0);

    public LimitedIterator(final Iterator<T> delegate, final Long limit) {
        this.delegate = delegate;
        this.limit = limit;
    }


    @Override protected T computeNext() {
        final long currentCount = count.incrementAndGet();
        if (currentCount > limit || !delegate.hasNext()) {
            endOfData();
            return null;
        } else {
            return delegate.next();
        }
    }
}
