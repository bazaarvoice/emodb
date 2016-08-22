package com.bazaarvoice.emodb.common.api;

import org.joda.time.Duration;

import static com.google.common.base.Preconditions.checkArgument;
import static org.joda.time.DateTimeConstants.MILLIS_PER_SECOND;

public class Ttls {

    public static Integer toSeconds(Duration ttl, int minimum, Integer forever) {
        if (ttl == null) {
            return forever;
        }
        long millis = ttl.getMillis();
        checkArgument(millis >= 0, "Ttl may not be negative: {}", ttl);

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
