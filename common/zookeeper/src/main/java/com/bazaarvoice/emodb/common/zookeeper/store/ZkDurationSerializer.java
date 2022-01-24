package com.bazaarvoice.emodb.common.zookeeper.store;

import com.google.common.base.CharMatcher;

import java.time.Duration;

/** Formats a duration in ZooKeeper as a human-readable ISO-8601 string for transparency, easy debugging. */
public class ZkDurationSerializer implements ZkValueSerializer<Duration> {
    @Override
    public String toString(Duration duration) {
        return duration.toString();
    }

    @Override
    public Duration fromString(String string) {
        if (CharMatcher.DIGIT.matchesAllOf(string)) {
            // Milliseconds value (eg. "600000" for 10 minutes)
            return Duration.ofMillis(Long.parseLong(string));
        } else {
            // ISO-8601 duration/period (eg. "PT5M" for 5 minutes)
            return Duration.parse(string);
        }
    }
}
