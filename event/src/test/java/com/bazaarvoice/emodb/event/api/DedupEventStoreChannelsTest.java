package com.bazaarvoice.emodb.event.api;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class DedupEventStoreChannelsTest {
    @Test
    public void testIsolated() {
        DedupEventStoreChannels channels = DedupEventStoreChannels.isolated("__write:", "__read:");
        assertEquals(channels.writeChannel("test"), "__write:test");
        assertEquals(channels.readChannel("test"), "__read:test");
        assertEquals(channels.queueFromWriteChannel("__write:test"), "test");
        assertEquals(channels.queueFromWriteChannel("__read:test"), null);
        assertEquals(channels.queueFromReadChannel("__read:test"), "test");
        assertEquals(channels.queueFromReadChannel("__write:test"), null);
    }

    @Test
    public void testSharedWriteChannel() {
        DedupEventStoreChannels channels = DedupEventStoreChannels.sharedWriteChannel("__read:");
        assertEquals(channels.writeChannel("test"), "test");
        assertEquals(channels.readChannel("test"), "__read:test");
        assertEquals(channels.queueFromWriteChannel("test"), "test");
        assertEquals(channels.queueFromWriteChannel("__read:test"), null);
        assertEquals(channels.queueFromReadChannel("__read:test"), "test");
        assertEquals(channels.queueFromReadChannel("test"), null);
    }
}
