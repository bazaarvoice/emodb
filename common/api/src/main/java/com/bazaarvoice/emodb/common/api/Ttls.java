package com.bazaarvoice.emodb.common.api;

import java.time.Duration;

public class Ttls {

    public static Integer toSeconds(Duration ttl, int minimum, Integer forever) {
        if (ttl == null) {
            return forever;
        }
        if (ttl.compareTo(Duration.ZERO) < 0) {
            throw new IllegalArgumentException("Ttl may not be negative: " + ttl);
        }

        // Convert to seconds, rounding up.
        long seconds = ttl.plusSeconds(1).minusMillis(1).getSeconds();

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
