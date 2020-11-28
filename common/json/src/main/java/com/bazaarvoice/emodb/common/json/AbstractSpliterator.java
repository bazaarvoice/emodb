package com.bazaarvoice.emodb.common.json;

import java.util.Spliterators;
import java.util.function.Consumer;

abstract public class AbstractSpliterator<T> extends Spliterators.AbstractSpliterator<T> {

    private boolean _endOfStream;

    public AbstractSpliterator() {
        this(Long.MAX_VALUE, 0);
    }

    public AbstractSpliterator(long est, int additionalCharacteristics) {
        super(est, additionalCharacteristics);
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        if (!_endOfStream) {
            T next = computeNext();
            if (!_endOfStream) {
                action.accept(next);
                return true;
            }
        }
        return false;
    }

    protected abstract T computeNext();

    protected final T endOfStream() {
        _endOfStream = true;
        return null;
    }
}
