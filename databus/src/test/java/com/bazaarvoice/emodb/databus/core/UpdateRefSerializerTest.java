package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class UpdateRefSerializerTest {
    @Test
    public void testRoundTrip() {
        UpdateRef expected = new UpdateRef("test-table", "test-key", TimeUUIDs.newUUID(), ImmutableSet.<String>of());
        ByteBuffer buf = UpdateRefSerializer.toByteBuffer(expected);
        UpdateRef actual = UpdateRefSerializer.fromByteBuffer(buf);

        verifyUpdateRefSerDe(expected, buf, actual);

        expected = new UpdateRef("test-table", "test-key", TimeUUIDs.newUUID(), ImmutableSet.of("ignore"));
        buf = UpdateRefSerializer.toByteBuffer(expected);
        actual = UpdateRefSerializer.fromByteBuffer(buf);

        verifyUpdateRefSerDe(expected, buf, actual);
    }

    private void verifyUpdateRefSerDe(UpdateRef expected, ByteBuffer buf, UpdateRef actual) {
        assertEquals(actual.getTable(), expected.getTable());
        assertEquals(actual.getKey(), expected.getKey());
        assertEquals(actual.getChangeId(), expected.getChangeId());
        assertEquals(actual.getTags(), expected.getTags());
        assertEquals(buf.remaining(), 0);  // Deserialization consumes the ByteBuffer
    }

    /** By default, serialized Astyanax Composite objects use a lot of memory (8192 bytes). */
    @Test
    public void testCapacity() {
        UpdateRef ref = new UpdateRef("test-table", "test-key", TimeUUIDs.newUUID(), ImmutableSet.of("aaaignore","aaaignore","aaaignore"));
        ByteBuffer buf = UpdateRefSerializer.toByteBuffer(ref);

        assertTrue(buf.capacity() < 128, Integer.toString(buf.capacity()));
    }
}
