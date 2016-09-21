package com.bazaarvoice.emodb.common.dropwizard.time;

import com.google.common.base.Ticker;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of {@link Ticker} using the current time as returned by {@link Clock#millis()}
 * as the return value for {@link Ticker#read()}.  Because of this the returned value will be in nanoseconds per the
 * Ticker spec but will only have millisecond resolution.
 *
 * On its own there is no real benefit of ClockTicker over {@link Ticker#systemTicker()}; both implementations
 * provide elapsed time from a fixed point and ClockTicker only provides millisecond precision while system ticker
 * may provide nanosecond precision.  The benefit comes from using ClockTicker in conjunction with Clock
 * for classes and processes that require both.  For example, consider the following class:
 *
 * <code>
 * public class SampleClass {
 *
 *     private LoadingCache<String, Integer> _lastMinuteCountCache;
 *
 *     public SampleClass(Clock clock) {
 *         _lastMinuteCountCache = CacheBuilder.newBuilder()
 *                 .ticker(ClockTicker.getTicker(clock))
 *                 .expireAfterAccess(1, TimeUnit.MINUTES)
 *                 .build(new CacheLoader<String, Integer>() {
 *                     public Integer load(String key) throws Exception {
 *                         return getCountSince(key, clock.millis());
 *                     }
 *                 });
 *     }
 *  ...
 * }
 * </code>
 *
 * This class requires the current time for its business logic and a ticker for cache expiration.  By providing a
 * Clock instance and using a ClockTicker tests can be built which consistently use the time provided
 * by <code>clock</code> to maintain a consistent timeline.
 */
public final class ClockTicker {

    private static final ClockTickerImpl DEFAULT = new ClockTickerImpl(Clock.systemUTC());

    public static Ticker getDefault() {
        return DEFAULT;
    }

    public static Ticker getTicker(Clock clock) {
        // Return the singleton instance for the default UTC clock
        if (Clock.systemUTC().equals(clock)) {
            return DEFAULT;
        }
        return new ClockTickerImpl(clock);
    }

    private static class ClockTickerImpl extends Ticker {
        private final Clock _clock;

        private ClockTickerImpl(Clock clock) {
            _clock = checkNotNull(clock, "clock");
        }

        @Override
        public long read() {
            return TimeUnit.MILLISECONDS.toNanos(_clock.millis());
        }
    }
}
