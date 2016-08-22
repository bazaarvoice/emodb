package com.bazaarvoice.emodb.common.zookeeper.store;

import com.google.common.base.CharMatcher;
import org.joda.time.Duration;
import org.joda.time.format.ISOPeriodFormat;
import org.joda.time.format.PeriodFormatter;

/** Formats a duration in ZooKeeper as a human-readable ISO-8601 string for transparency, easy debugging. */
public class ZkDurationSerializer implements ZkValueSerializer<Duration> {
    private static final PeriodFormatter ISO_FORMAT = ISOPeriodFormat.standard();

    @Override
    public String toString(Duration duration) {
        return duration.toPeriod().toString();
    }

    @Override
    public Duration fromString(String string) {
        if (CharMatcher.DIGIT.matchesAllOf(string)) {
            // Milliseconds value (eg. "600000" for 10 minutes)
            return Duration.millis(Long.parseLong(string));
        } else {
            // ISO-8601 duration/period (eg. "PT5M" for 5 minutes)
            return ISO_FORMAT.parsePeriod(string).toStandardDuration();
        }
    }
}
