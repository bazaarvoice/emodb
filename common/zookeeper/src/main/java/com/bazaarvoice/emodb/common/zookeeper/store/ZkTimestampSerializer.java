package com.bazaarvoice.emodb.common.zookeeper.store;

import com.google.common.base.CharMatcher;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

/** Formats a timestamp in ZooKeeper as a human-readable ISO-8601 string for transparency, easy debugging. */
public class ZkTimestampSerializer implements ZkValueSerializer<Long> {
    private static final DateTimeFormatter ISO_FORMATTER = ISODateTimeFormat.dateTime().withZoneUTC();
    private static final DateTimeFormatter ISO_PARSER = ISODateTimeFormat.dateTimeParser().withZoneUTC();

    @Override
    public String toString(Long timestamp) {
        return ISO_FORMATTER.print(timestamp);
    }

    @Override
    public Long fromString(String string) {
        if (CharMatcher.DIGIT.matchesAllOf(string)) {
            // Milliseconds value (eg. "1382620121882" for 2013-10-24 13:08:41.882 UTC)
            return Long.parseLong(string);
        } else {
            // ISO-8601 timestamp (eg. "2013-10-24T13:08:41.882Z")
            return ISO_PARSER.parseDateTime(string).getMillis();
        }
    }
}
