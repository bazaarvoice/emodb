package com.bazaarvoice.emodb.common.zookeeper.store;

import com.google.common.base.CharMatcher;

import java.time.Instant;
import java.time.ZonedDateTime;

/** Formats a timestamp in ZooKeeper as a human-readable ISO-8601 string for transparency, easy debugging. */
public class ZkTimestampSerializer implements ZkValueSerializer<Long> {

    @Override
    public String toString(Long timestamp) {
        return Instant.ofEpochMilli(timestamp).toString();
    }

    @Override
    public Long fromString(String string) {
        if (CharMatcher.digit().matchesAllOf(string)) {
            // Milliseconds value (eg. "1382620121882" for 2013-10-24 13:08:41.882 UTC)
            return Long.parseLong(string);
        } else {
            // ISO-8601 timestamp (eg. "2013-10-24T13:08:41.882Z")
            return ZonedDateTime.parse(string).toInstant().toEpochMilli();
        }
    }
}
