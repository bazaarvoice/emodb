package com.bazaarvoice.emodb.event.db.astyanax;

import com.bazaarvoice.emodb.event.db.EventId;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Wraps a 16-byte slab id, 2 byte event index, 2 byte checksum.  The checksum is to help find bugs where external
 * clients mix up which event id belongs to which channel.
 */
class AstyanaxEventId implements EventId {
    private final byte[] _buf;
    private final String _channel;

    static AstyanaxEventId parse(byte[] buf, String channel) {
        checkArgument(buf.length == 20, "Invalid event ID.");

        // Verify the checksum in the last 2 bytes
        int actualChecksum = getUnsignedShort(buf, 18);
        int expectedChecksum = computeChecksum(buf, 0, 18, channel);
        checkArgument(actualChecksum == expectedChecksum, "Invalid event ID checksum.");

        return new AstyanaxEventId(buf, channel);
    }

    static AstyanaxEventId create(String channel, ByteBuffer slabId, int eventIdx) {
        requireNonNull(channel);
        requireNonNull(slabId);
        checkArgument(slabId.remaining() == 16);
        checkArgument(eventIdx >= 0 && eventIdx <= 0xffff);  // Must fit in 2 bytes

        byte[] buf = new byte[20];
        // First 16 bytes is the slab id
        slabId.duplicate().get(buf, 0, 16);

        // Next 2 bytes is the event index
        putUnsignedShort(eventIdx, buf, 16);

        // Last 2 bytes is a checksum of the first 18 bytes and the channel to make sure ids don't get corrupted
        int checksum = computeChecksum(buf, 0, 18, channel);
        putUnsignedShort(checksum, buf, 18);

        return new AstyanaxEventId(buf, channel);
    }

    private AstyanaxEventId(byte[] buf, String channel) {
        checkArgument(buf.length == 20, "Invalid event ID.");
        _buf = buf;
        _channel = requireNonNull(channel);
    }

    @Override
    public byte[] array() {
        return _buf;
    }

    String getChannel() {
        return _channel;
    }

    ByteBuffer getSlabId() {
        return ByteBuffer.wrap(_buf, 0, 16);
    }

    int getEventIdx() {
        return getUnsignedShort(_buf, 16);
    }

    /** Computes a 16-bit checksum of the contents of the specific ByteBuffer and channel name. */
    private static int computeChecksum(byte[] buf, int offset, int length, String channel) {
        Hasher hasher = Hashing.murmur3_32().newHasher();
        hasher.putBytes(buf, offset, length);
        hasher.putUnencodedChars(channel);
        return hasher.hash().asInt() & 0xffff;
    }

    private static void putUnsignedShort(int value, byte[] buf, int offset) {
        checkArgument((value & ~0xffff) == 0);
        buf[offset]     = (byte) (value >>> 8);
        buf[offset + 1] = (byte) (value);
    }

    private static int getUnsignedShort(byte[] buf, int offset) {
        return ((buf[offset] & 0xff) << 8) | (buf[offset + 1] & 0xff);
    }
}
