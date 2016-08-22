package com.bazaarvoice.emodb.event.db.astyanax;

import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Random;

import static org.testng.Assert.assertEquals;

public class AstyanaxEventIdTest {
    private static final Random _random = new Random();

    @Test
    public void testEventIdRoundTrip() {
        String channel = "channel";
        ByteBuffer slabId = randomBytes(16);
        int eventIdx = _random.nextInt(0xffff);

        byte[] buf = AstyanaxEventId.create(channel, slabId, eventIdx).array();
        AstyanaxEventId eventId = AstyanaxEventId.parse(buf, channel);

        assertEquals(eventId.getChannel(), channel);
        assertEquals(eventId.getSlabId(), slabId);
        assertEquals(eventId.getEventIdx(), eventIdx);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testCorruptedEventId() {
        // This test must be deterministic so checksum collisions don't cause spurious failures.
        Random random = new Random(1234567);
        byte[] buf = AstyanaxEventId.create("channel", randomBytes(random, 16), random.nextInt(0xffff)).array();
        buf[0] = (byte)(buf[0] + 1);
        AstyanaxEventId.parse(buf, "channel");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testShortEventId() {
        AstyanaxEventId.parse(new byte[19], "channel");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLongEventId() {
        AstyanaxEventId.parse(new byte[21], "channel");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testSmallEventIdx() {
        AstyanaxEventId.create("channel", randomBytes(16), -1);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLargeEventIdx() {
        AstyanaxEventId.create("channel", randomBytes(16), 0x10000);
    }

    private ByteBuffer randomBytes(int len) {
        return randomBytes(_random, len);
    }

    private ByteBuffer randomBytes(Random random, int len) {
        byte[] buf = new byte[len];
        random.nextBytes(buf);
        return ByteBuffer.wrap(buf);
    }
}
