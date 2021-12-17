package com.bazaarvoice.emodb.common.dropwizard.guava;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Extension of {@link Suppliers}.
 */
public final class MoreSuppliers {

    private MoreSuppliers() {
        // empty
    }

    /**
     * Memoization similar to {@link Suppliers#memoizeWithExpiration(Supplier, long, TimeUnit)} except the expiration
     * is randomly selected to be a value within provided bounds.  For example:
     *
     * <code>
     *     Supplier&lt;Object&t; supplier = MoreSuppliers.memoizeWithRandomExpiration(delegate, 5, 10, TimeUnit.SECONDS);
     * </code>
     *
     * returns a Supplier that will memoize the delegate's response for a random number of nanoseconds between 5
     * (inclusive) to 10 (exclusive) seconds.
     *
     * This is useful when numerous memoized values are typically computed independently but used within the same operation.
     * If all values were computed and memoized together then each call that refreshes from delegate could take longer
     * than desirable.  Similarly, if each value were memomized independently using the same expiration then herding
     * behavior would result in the same long calls at expiration time.  A random expiration allows each value to be
     * cached but spread out the refresh cycle so that it is unlikely that any single call will refresh the entire value
     * set, so long as the call frequency is significantly below <code>minDuration</code>.
     */
    public static <T> Supplier<T> memoizeWithRandomExpiration(Supplier<T> delegate, long minDuration, long maxDuration, TimeUnit units) {
        if (minDuration == maxDuration) {
            // This case resolves to standard expiration
            return Suppliers.memoizeWithExpiration(delegate, minDuration, units);
        }
        return new RandomExpirationSupplier<T>(delegate, minDuration, maxDuration, units);
    }

    /**
     * Class modeled off of {@link Suppliers.ExpiringMemoizingSupplier} except the expiration time is randomly
     * selected within the provided bounds for each iteration.
     */
    private final static class RandomExpirationSupplier<T> implements Supplier<T> {

        private final Supplier<T> _delegate;
        private final PrimitiveIterator.OfLong _nanosDurations;
        private transient volatile T _value;
        private transient volatile long _expirationNanos = 0;

        RandomExpirationSupplier(Supplier<T> delegate, long minDuration, long maxDuration, TimeUnit timeUnit) {
            checkArgument(minDuration >= 0, "minDuration cannot be negative");
            checkArgument(maxDuration > minDuration, "maxDuration must be greater than minDuration");
            requireNonNull(delegate, "delegate");
            requireNonNull(timeUnit, "timeUnit");

            long minDurationNanos = timeUnit.toNanos(minDuration);
            long maxDurationNanos = timeUnit.toNanos(maxDuration);

            _delegate = delegate;
            _nanosDurations = new Random().longs(minDurationNanos, maxDurationNanos).iterator();
        }

        @Override
        public T get() {
            long expirationNanos = _expirationNanos;
            long now = System.nanoTime();

            if (expirationNanos == 0 || now - expirationNanos >= 0) {
                synchronized(this) {
                    if (expirationNanos == _expirationNanos) {
                        _value = _delegate.get();
                        expirationNanos = now + _nanosDurations.nextLong();
                    }
                    _expirationNanos = expirationNanos == 0 ? 1 : expirationNanos;
                }
            }

            return _value;
        }
    }
}
