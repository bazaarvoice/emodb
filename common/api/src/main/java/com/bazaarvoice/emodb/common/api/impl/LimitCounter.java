package com.bazaarvoice.emodb.common.api.impl;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

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
        checkArgument(initialValue > 0, "Limit must be >0");
        _remaining = initialValue;
    }

    public long remaining() {
        checkState(_remaining > 0);  // The Iterator never computes iter.hasNext() when _remaining <= 0.
        return _remaining;
    }

    /**
     * Similar to {@link Iterators#limit(Iterator, int)} except that it updates this limit counter object so other
     * code can track how many items are remaining before the limit is reached.  This is useful when the underlying
     * iterator queries in batch because it can adjust its batch sizes to avoid going too far past the limit.
     */
    public <T> Iterator<T> limit(final Iterator<T> iter) {
        return new AbstractIterator<T>() {
            @Override
            protected T computeNext() {
                if (_remaining > 0 && iter.hasNext()) {
                    _remaining--;
                    return iter.next();
                }
                return endOfData();
            }
        };
    }

    @Override
    public String toString() {
        return Long.toString(_remaining); // for debugging
    }
}
