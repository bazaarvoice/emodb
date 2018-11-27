package com.bazaarvoice.emodb.common.uuid;

import org.testng.annotations.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TimeUUIDsTest {

    /** Minimum UUID timestamp possible (uuid epoch start, date the Gregorian calendar started. */
    private static final Date TIMESTAMP_MIN = parseTimestamp("1582-10-15 00:00:00.000 UTC");

    /** Maximum UUID timestamp possible when using millisecond precision. */
    private static final Date TIMESTAMP_MAX = parseTimestamp("5236-03-31 21:21:00.684 UTC");

    @Test
    public void testDates() {
        // we can extract a date from a UUID created with the same date
        Date now = new Date();
        UUID nowUuid = TimeUUIDs.uuidForTimestamp(now);
        assertEquals(nowUuid.version(), 1);
        assertEquals(nowUuid.node(), 0);
        assertEquals(nowUuid.clockSequence(), 0);
        assertEquals(TimeUUIDs.getDate(nowUuid), now);

        // creating a new UUID uses the current time
        Date start = new Date();
        UUID uuid = TimeUUIDs.newUUID();
        Date end = new Date();
        Date date = TimeUUIDs.getDate(uuid);
        assertTrue(start.compareTo(date) <= 0, "Expected: " + start.getTime() + " <= Actual: " + date.getTime());
        assertTrue(end.compareTo(date) >= 0, "Expected: " + end.getTime() + " >= Actual: " + date.getTime());
    }

    @Test
    public void testCompare() throws Exception {
        for (int i = 0; i < 10; i++) {
            TimeUUIDs.newUUID();
        }
        UUID uuid1 = TimeUUIDs.newUUID();
        UUID uuid2 = TimeUUIDs.newUUID();
        assertLessThan(uuid1, uuid2);

        UUID uuid3 = TimeUUIDs.newUUID();
        Thread.sleep(10);
        UUID uuid4 = TimeUUIDs.newUUID();
        assertLessThan(uuid3, uuid4);
    }

    @Test
    public void testMillisAndSequence() {
        long now = System.currentTimeMillis();
        UUID uuid1 = TimeUUIDs.uuidForTimeMillis(now, 1);
        UUID uuid0 = TimeUUIDs.uuidForTimeMillis(now, 0);
        UUID uuid2 = TimeUUIDs.uuidForTimeMillis(now, 2);
        assertLessThan(uuid0, uuid1);
        assertLessThan(uuid1, uuid2);
    }

    @Test
    public void testMillisLowerBound() {
        UUID uuid = TimeUUIDs.uuidForTimestamp(TIMESTAMP_MIN);
        assertEquals(uuid.toString(), "00000000-0000-1000-8000-000000000000");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMillisBelowLowerBound() {
        TimeUUIDs.uuidForTimeMillis(TIMESTAMP_MIN.getTime() - 1);
    }

    @Test
    public void testMillisUpperBound() {
        UUID uuid = TimeUUIDs.uuidForTimestamp(TIMESTAMP_MAX);
        assertEquals(uuid.toString(), "ffffe4c0-ffff-1fff-8000-000000000000");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testMillisAboveUpperBound() {
        TimeUUIDs.uuidForTimeMillis(TIMESTAMP_MAX.getTime() + 1);
    }

    @Test
    public void testMinimumUuid() {
        UUID minUuid = TimeUUIDs.minimumUuid();

        assertEquals(minUuid.toString(), "00000000-0000-1000-8000-000000000000");

        // Compare against today
        assertLessThan(minUuid, TimeUUIDs.newUUID());
        assertLessThan(minUuid, TimeUUIDs.uuidForTimestamp(new Date()));

        // Compare against a value in the far past
        assertEquals(minUuid, TimeUUIDs.uuidForTimestamp(TIMESTAMP_MIN));

        // Compare against a value in the far future (check for under/overflow etc.)
        assertLessThan(minUuid, TimeUUIDs.uuidForTimestamp(TIMESTAMP_MAX));
    }

    @Test
    public void testMaximumUuid() {
        UUID maxUuid = TimeUUIDs.maximumUuid();

        assertEquals(maxUuid.toString(), "ffffffff-ffff-1fff-bfff-ffffffffffff");

        // Compare against today
        assertLessThan(TimeUUIDs.newUUID(), maxUuid);
        assertLessThan(TimeUUIDs.uuidForTimestamp(new Date()), maxUuid);

        // Compare against a value in the far past (check for under/overflow etc.)
        assertLessThan(TimeUUIDs.uuidForTimestamp(TIMESTAMP_MIN), maxUuid);

        // Compare against a value in the far future
        assertLessThan(TimeUUIDs.uuidForTimestamp(TIMESTAMP_MAX), maxUuid);
    }

    @Test
    public void testRFC4122Compliance() {
        assertRFC4122TimeUUID(TimeUUIDs.newUUID());
        assertRFC4122TimeUUID(TimeUUIDs.uuidForTimestamp(new Date(), Integer.MAX_VALUE));
        assertRFC4122TimeUUID(TimeUUIDs.minimumUuid());
        assertRFC4122TimeUUID(TimeUUIDs.maximumUuid());
    }

    @Test
    public void testNext() {
        // Max UUID
        assertNull(TimeUUIDs.getNext(TimeUUIDs.maximumUuid()));

        // Min UUID
        assertEquals(TimeUUIDs.getNext(TimeUUIDs.minimumUuid()).toString(), "00000000-0000-1000-8000-000000000001");

        // Increment LSB
        assertEquals(TimeUUIDs.getNext(UUID.fromString("3a9f0da0-9b99-11e2-8000-000000000000")).toString(), "3a9f0da0-9b99-11e2-8000-000000000001");

        // Increment MSB
        assertEquals(TimeUUIDs.getNext(UUID.fromString("3a9f0da0-9b99-11e2-bfff-ffffffffffff")).toString(), "3a9f0da1-9b99-11e2-8000-000000000000");

        UUID uuid = TimeUUIDs.newUUID();
        assertLessThan(uuid, TimeUUIDs.getNext(uuid));

        long now = System.currentTimeMillis();
        assertNotEquals(TimeUUIDs.getTimeMillis(TimeUUIDs.getNext(TimeUUIDs.uuidForTimeMillis(now))), now + 1);
    }

    @Test
    public void testPrevious() {
        // Min UUID
        assertNull(TimeUUIDs.getPrevious(TimeUUIDs.minimumUuid()));

        // Max UUID
        assertEquals(TimeUUIDs.getPrevious(TimeUUIDs.maximumUuid()).toString(), "ffffffff-ffff-1fff-bfff-fffffffffffe");

        // Decrement LSB
        assertEquals(TimeUUIDs.getPrevious(UUID.fromString("3a9f0da0-9b99-11e2-8000-000000000001")).toString(), "3a9f0da0-9b99-11e2-8000-000000000000");

        // Decrement MSB
        assertEquals(TimeUUIDs.getPrevious(UUID.fromString("3a9f0da1-9b99-11e2-8000-000000000000")).toString(), "3a9f0da0-9b99-11e2-bfff-ffffffffffff");

        UUID uuid = TimeUUIDs.newUUID();
        assertLessThan(TimeUUIDs.getPrevious(uuid), uuid);

        long now = System.currentTimeMillis();
        assertEquals(TimeUUIDs.getTimeMillis(TimeUUIDs.getPrevious(TimeUUIDs.uuidForTimeMillis(now + 1))), now);
    }

    @Test
    public void testNextPreviousInverse() {
        UUID uuid = TimeUUIDs.newUUID();
        assertEquals(TimeUUIDs.getNext(TimeUUIDs.getPrevious(uuid)), uuid);
        assertEquals(TimeUUIDs.getPrevious(TimeUUIDs.getNext(uuid)), uuid);
    }

    private void assertRFC4122TimeUUID(UUID uuid) {
        assertEquals(uuid.version(), 1);  // timestamp uuid
        assertEquals(uuid.variant(), 2);  // RFC 4122 uuid
    }

    private void assertLessThan(UUID uuid0, UUID uuid1) {
        // Compare in both directions (a < b, b > a) to make sure the comparator is symmetric
        assertTrue(TimeUUIDs.comparator().compare(uuid0, uuid1) < 0, uuid0 + " should be < " + uuid1);
        assertTrue(TimeUUIDs.comparator().compare(uuid1, uuid0) > 0, uuid1 + " should be > " + uuid0);
    }

    private static Date parseTimestamp(String string) {
        try {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS z").parse(string);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}
