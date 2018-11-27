package com.bazaarvoice.emodb.common.api;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class Ttls {

    private static final long MILLIS_PER_SECOND = TimeUnit.SECONDS.toMillis(1);

    public static Integer toSeconds(Duration ttl, int minimum, Integer forever) {
        if (ttl == null) {
            return forever;
        }
        long millis = ttl.toMillis();
        if (millis < 0) {
            throw new IllegalArgumentException("Ttl may not be negative: " + ttl);
        }

        // Convert to seconds, rounding up.
        long seconds = (millis + MILLIS_PER_SECOND - 1) / MILLIS_PER_SECOND;

        // No support for really large numbers, convert to forever.
        if (seconds > Integer.MAX_VALUE) {
            return forever;
        }

        // Constrain to min/max
        if (seconds < minimum) {
            return minimum;
        }
        if (forever != null && seconds > forever) {
            return forever;
        }

        return (int) seconds;
    }
}
