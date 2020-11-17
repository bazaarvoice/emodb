package com.bazaarvoice.emodb.common.api.impl;

import java.util.Collections;
import java.util.Iterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

/**
 * A mutable limit value.  At any given time the {@link #remaining()} method will return the remaining number of records
 * desired.  This means that, usually, as a client iterates through a result set, it should decrement the limit after
 * each row.  However, if the client ignores a particular row for some reason (eg. filtered / deleted / range ghost) it
 * can skip decrementing the limit with the result that the iterator may, perhaps, end up returning more rows than
 * originally requested.
 */
public class LimitCounter {
    private long _remaining;

    public static LimitCounter max() {
        return new LimitCounter(Long.MAX_VALUE);
    }

    public LimitCounter(long initialValue) {
        if (initialValue <= 0) {
            throw new IllegalArgumentException("Limit must be >0");
        }
        _remaining = initialValue;
    }

    public long remaining() {
        if (_remaining < 0) {
            throw new IllegalStateException("Negative remaining value");
        }
        return _remaining;
    }

    /**
     * Similar to Guava's <code>Iterators.limit()</code> except that it updates this limit counter object so other
     * code can track how many items are remaining before the limit is reached.  This is useful when the underlying
     * iterator queries in batch because it can adjust its batch sizes to avoid going too far past the limit.
     */
    public <T> Iterator<T> limit(final Iterator<T> iter) {
        final long remaining = _remaining;
        if (remaining == 0) {
            return Collections.emptyIterator();
        }
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, 0), false)
                .limit(remaining)
                .peek(ignore -> _remaining -= 1)
                .iterator();
    }

    @Override
    public String toString() {
        return Long.toString(_remaining); // for debugging
    }
}
