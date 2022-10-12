package com.bazaarvoice.emodb.sor.client2;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;

import java.time.Duration;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Wraps an iterator such that {@link #hasNext()} returns {@code false} after a certain period of time.
 * <p>
 * A minimum may be specified such that the time limit is ignored until at least {@code N} items have been returned.
 */
class TimeLimitedIterator<T> extends AbstractIterator<T> implements PeekingIterator<T> {
    private final Iterator<T> _iterator;
    private final long _expireAt;
    private long _minimum;

    static <T> PeekingIterator<T> create(Iterator<T> iterator, Duration duration, long minimum) {
        return new TimeLimitedIterator<>(iterator, duration, minimum);
    }

    private TimeLimitedIterator(Iterator<T> iterator, Duration duration, long minimum) {
        _iterator = requireNonNull(iterator, "iterator");
        _expireAt = System.currentTimeMillis() + duration.toMillis();
        checkArgument(minimum >= 0, "Minimum must be >= 0");
        _minimum = minimum;
    }

    @Override
    protected T computeNext() {
        if (_iterator.hasNext() && (_minimum > 0 || System.currentTimeMillis() < _expireAt)) {
            if (_minimum > 0) {
                _minimum--;
            }
            return _iterator.next();
        }
        return endOfData();
    }
}
