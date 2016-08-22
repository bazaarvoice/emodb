package com.bazaarvoice.emodb.common.uuid;

import com.eaio.uuid.UUIDGen;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

import java.util.Date;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Operations on time-based UUIDs.
 */
public abstract class TimeUUIDs {
    /** Magic number from com.eaio.uuid.UUID.createTime(). */
    private static final long NUM_100NS_INTERVALS_SINCE_UUID_EPOCH = 0x01b21dd213814000L;

    /** Minimum time in milliseconds that doesn't underflow a time UUID, equal to 12am Oct 15, 1582 (UTC). */
    private static final long TIME_MILLIS_MIN = -NUM_100NS_INTERVALS_SINCE_UUID_EPOCH / 10000;
    /** Maximum time in milliseconds that doesn't overflow a time UUID, equal to 9:21pm Mar 31, 5236 (UTC). */
    private static final long TIME_MILLIS_MAX = ((-1L >>> 4) - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;

    private static final Ordering<UUID> ORDERING = new Ordering<UUID>() {
        @Override
        public int compare(UUID uuid1, UUID uuid2) {
            return TimeUUIDs.compare(uuid1, uuid2);
        }
    };

    /** Prevent instantiation. */
    private TimeUUIDs() {}

    public static UUID newUUID() {
        // from http://wiki.apache.org/cassandra/FAQ#working_with_timeuuid_in_java
        // seems to have a few weaknesses:
        // - implicitly assumes a single JVM per mac address.  Math.random() is
        //   used to vary a bit, but seed collisions are possible.  why not SecureRandom,
        //   a guaranteed unique # from zookeeper, or just listen on a port for the lifetime
        //   of the JVM and use the port number for the "seq" field in the "seqAndNode" part.
        // - uses System.currentTimeMillis for the time.  why not System.nanoTime()?
        // - createTime() is "static synchronized", could be a bottleneck?
        return new UUID(UUIDGen.newTime(), UUIDGen.getClockSeqAndNode());
    }

    /**
     * Fabricates a non-unique UUID value for the specified timestamp.  Do NOT use this as
     * a unique identifier.  It's only useful for performing range comparisons using
     * a TimeUUID-aware comparator.
     */
    public static UUID uuidForTimestamp(Date timestamp) {
        return uuidForTimeMillis(timestamp.getTime(), 0);
    }

    /**
     * Fabricates a non-unique UUID value for the specified timestamp that sorts relative to other similar UUIDs based
     * on the value of {@code sequence}.  Be very careful about using this as a unique identifier since the usual
     * protections against conflicts do not apply.  At most, only assume this is unique within a specific context
     * where the sequence number is unique w/in that context.
     */
    public static UUID uuidForTimestamp(Date timestamp, int sequence) {
        return uuidForTimeMillis(timestamp.getTime(), sequence);
    }

    /**
     * Fabricates a non-unique UUID value for the specified timestamp.  Do NOT use this as
     * a unique identifier.  It's only useful for performing range comparisons using
     * a TimeUUID-aware comparator.
     */
    public static UUID uuidForTimeMillis(long timeMillis) {
        return uuidForTimeMillis(timeMillis, 0);
    }

    /**
     * Fabricates a non-unique UUID value for the specified timestamp that sorts relative to other similar UUIDs based
     * on the value of {@code sequence}.  Be very careful about using this as a unique identifier since the usual
     * protections against conflicts do not apply.  At most, only assume this is unique within a specific context
     * where the sequence number is unique w/in that context.
     */
    public static UUID uuidForTimeMillis(long timeMillis, int sequence) {
        long time = getRawTimestamp(timeMillis);
        return new UUID(getMostSignificantBits(time), getLeastSignificantBits(sequence, 0));
    }

    /**
     * Returns a UUID that sorts less than or equal to all RFC 4122 timestamp UUIDs (version = 1, variant = 2).
     */
    public static UUID minimumUuid() {
        return new UUID(getMostSignificantBits(0), getLeastSignificantBits(0, 0));
    }

    /**
     * Returns a UUID that sorts greater than or equal to all RFC 4122 timestamp UUIDs (version = 1, variant = 2).
     */
    public static UUID maximumUuid() {
        return new UUID(getMostSignificantBits(Long.MAX_VALUE),
                getLeastSignificantBits(Integer.MAX_VALUE, Long.MAX_VALUE));
    }

    private static long getMostSignificantBits(long rawTime) {
        // | high (12 bits) | version "0001" (4 bit) | mid (16 bits) | low (32-bits) |
        long version = (1 << 12);
        long timeLow = (rawTime &       0xffffffffL) << 32;
        long timeMid = (rawTime &   0xffff00000000L) >> 16;
        long timeHi = (rawTime & 0xfff000000000000L) >> 48;
        return timeLow | timeMid | version | timeHi;
    }

    /**
     * Fabricates a clock sequence and node value such that values sort based on the @{code sequence} parameter,
     * as long as sequence is < 2^14 (16384).
     */
    private static long getLeastSignificantBits(int sequence, long node) {
        // | reserved (2 bits) | clk_seq_hi (6 bits) | clk_seq_lo (8 bits) | node (48 bits) |
        return 0x8000000000000000L | (((long) (sequence & 0x3fff)) << 48) | (node & 0xffffffffffffL);
    }

    /**
     * Returns the Date as the number of 100ns intervals since 00:00:00.00,
     * 15 October 1582 UTC, rounded to a millisecond boundary.
     */
    private static long getRawTimestamp(long timeMillis) {
        checkArgument(timeMillis >= TIME_MILLIS_MIN, "Time value too small.");  // < 12am Oct 15, 1582 (UTC)
        checkArgument(timeMillis <= TIME_MILLIS_MAX, "Time value too large.");  // > 9:21pm Mar 31, 5236 (UTC)
        return timeMillis * 10000 + NUM_100NS_INTERVALS_SINCE_UUID_EPOCH;
    }

    /**
     * Returns the timestamp when the UUID was created, truncated to millisecond
     * boundary.  Note that it's theoretically possible that UUIDs could be
     * generated fast enough by a single JVM that the resulting time is too
     * recent since the time component doubles as the high frequency counter
     * used to ensure all UUID generated by a JVM are distinct.
     * @throws UnsupportedOperationException if the uuid is not a timestamp UUID
     */
    public static Date getDate(UUID uuid) {
        return new Date(getTimeMillis(uuid));
    }

    /**
     * Returns the timestamp when the UUID was created, truncated to millisecond
     * boundary.
     * @throws UnsupportedOperationException if the uuid is not a timestamp UUID
     */
    public static long getTimeMillis(UUID uuid) {
        return (uuid.timestamp() - NUM_100NS_INTERVALS_SINCE_UUID_EPOCH) / 10000;
    }

    /**
     * Compare two time UUIDs deterministically, first by their embedded timestamp,
     * next by their node-specific sequence number, finally breaking ties using
     * their embedded node identifier.\
     * @throws UnsupportedOperationException if either uuid is not a timestamp UUID
     */
    public static int compare(UUID uuid1, UUID uuid2) {
        int timeResult = Longs.compare(uuid1.timestamp(), uuid2.timestamp());
        if (timeResult != 0) {
            return timeResult;
        }
        // if the time component is identical, break ties using the clockSeqAndNode
        // component (the 64 least significant bits).  the top 16 bits of the lsb are
        // the "clockSeq" number which, theoretically, is an incrementing counter.  in
        // practice, however, (and in com.eaio.uuid) clockSeq is a random number and
        // shouldn't be interpreted as meaning anything.  just use it for breaking ties.
        return uuid1.compareTo(uuid2);
    }

    /**
     * Compare the embedded timestamps of the given UUIDs. This is used when it is OK to return
     * an equality based on timestamps alone
     * @throws UnsupportedOperationException if either uuid is not a timestamp UUID
     */
    public static int compareTimestamps(UUID uuid1, UUID uuid2) {
        return Longs.compare(uuid1.timestamp(), uuid2.timestamp());
    }

    /**
     * Sort time UUIDs deterministically such that, as much as can be determined from the
     * information in the UUID, UUIDs created earlier sort before UUIDs created later.
     */
    public static Ordering<UUID> ordering() {
        return ORDERING;
    }

    public static <T> NavigableMap<UUID, T> sortedCopy(Map<UUID, T> map) {
        return ImmutableSortedMap.copyOf(map, ordering());
    }

    /**
     * Returns the smallest valid time UUID that is greater than the specified time uuid based on
     * {@link UUID#compareTo(java.util.UUID)}}, or <code>null</code> if the uuid is greater than or
     * equal to {@link #maximumUuid()}.
     */
    public static UUID getNext(UUID uuid) {
        checkArgument(uuid.version() == 1, "Not a time UUID");
        UUID max = maximumUuid();
        long lsb = uuid.getLeastSignificantBits();
        if (lsb < max.getLeastSignificantBits()) {
            return new UUID(uuid.getMostSignificantBits(), lsb + 1);
        }
        long timestamp = uuid.timestamp();
        if (timestamp < max.timestamp()) {
            return new UUID(getMostSignificantBits(timestamp + 1), minimumUuid().getLeastSignificantBits());
        }
        return null;  // No next exists since uuid == maximumUuid()
    }

    /**
     * Returns the largest valid time UUID that is less than the specified time uuid based on
     * {@link UUID#compareTo(java.util.UUID)}}, or <code>null</code> if the uuid is less than or
     * equal to {@link #minimumUuid()}.
     */
    public static UUID getPrevious(UUID uuid) {
        checkArgument(uuid.version() == 1, "Not a time UUID");
        UUID min = minimumUuid();
        long lsb = uuid.getLeastSignificantBits();
        if (lsb > min.getLeastSignificantBits()) {
            return new UUID(uuid.getMostSignificantBits(), lsb - 1);
        }
        long timestamp = uuid.timestamp();
        if (timestamp > min.timestamp()) {
            return new UUID(getMostSignificantBits(timestamp - 1), maximumUuid().getLeastSignificantBits());
        }
        return null;  // No previous exists since uuid == minimumUuid()
    }
}
