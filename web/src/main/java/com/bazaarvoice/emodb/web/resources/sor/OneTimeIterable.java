package com.bazaarvoice.emodb.web.resources.sor;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Wraps an {@code Iterator} as an {@code Iterable} that can only be used once.
 */
class OneTimeIterable<T> implements Iterable<T> {
    private final Iterator<T> _iterator;
    private boolean _consumed;

    static <T> Iterable<T> wrap(Iterator<T> iterator) {
        return new OneTimeIterable<>(iterator);
    }

    private OneTimeIterable(Iterator<T> iterator) {
        _iterator = checkNotNull(iterator, "iterator");
    }

    @Override
    public Iterator<T> iterator() {
        checkState(!_consumed);
        _consumed = true;
        return _iterator;
    }
}
