package com.bazaarvoice.emodb.sortedq.core;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.apache.commons.codec.binary.Base64;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public class SegmentSnapshotJsonTest {
    private static Random RANDOM = new Random();

    /** Basic json encoding/decoding with a segment in the NORMAL state. */
    @Test
    @SuppressWarnings("unchecked")
    public void testSnapshotJson() throws IOException {
        SplitQueue<Segment> splitQueue = mock(SplitQueue.class);

        Segment segment = new Segment(TimeUUIDs.newUUID(), intBuffer(1), 12345, splitQueue);
        segment.onRecordAdded(intBuffer(6789));
        segment.onRecordAdded(intBuffer(1234));
        segment.onRecordAdded(intBuffer(Integer.MIN_VALUE));
        segment.onRecordsDeleted(1);

        // Test encoding
        Segment.Snapshot expected = segment.snapshot();
        String string = JsonHelper.asJson(expected);
        String hllState = Base64.encodeBase64String(expected.distinctAddsHll);
        assertEquals(string, "{\"version\":1,\"dataId\":\"" + segment.getDataId() + "\",\"min\":\"00000001\"," +
                "\"adds\":3,\"bytesAdded\":12,\"distinctAddsHll\":\"" + hllState + "\",\"deletes\":1," +
                "\"bytesUntilSplitCheckSize\":771,\"splitting\":false,\"splitTargetSize\":0,\"splitTargetRemaining\":0}");

        // Test decoding
        Segment.Snapshot actual = JsonHelper.fromJson(string, Segment.Snapshot.class);
        assertEqualsSnapshot(actual, expected);

        // Verify we can create a segment that looks like it's in the same state as the original.
        Segment hydrated = new Segment(segment.getId(), actual, 12345, splitQueue);
        assertEqualsSegment(hydrated, segment);
    }

    /** Basic json encoding/decoding with a segment in the SPLITTING state, null value for "min". */
    @Test
    @SuppressWarnings("unchecked")
    public void testSnapshotJson2() throws IOException {
        SplitQueue<Segment> splitQueue = mock(SplitQueue.class);

        Segment segment = new Segment(TimeUUIDs.newUUID(), (ByteBuffer) null, 12345, splitQueue);
        segment.onRecordAdded(randomBuffer(20000));
        segment.onSplitWork(8000);
        segment.onSplitWork(1234);

        // Test encoding.
        Segment.Snapshot expected = segment.snapshot();
        String string = JsonHelper.asJson(expected);
        String hllState = Base64.encodeBase64String(expected.distinctAddsHll);
        assertEquals(string, "{\"version\":1,\"dataId\":\"" + segment.getDataId() + "\",\"min\":null," +
                "\"adds\":1,\"bytesAdded\":20000,\"distinctAddsHll\":\"" + hllState + "\",\"deletes\":0," +
                "\"bytesUntilSplitCheckSize\":771,\"splitting\":true,\"splitTargetSize\":5500,\"splitTargetRemaining\":4266}");

        // Test decoding.
        Segment.Snapshot actual = JsonHelper.fromJson(string, Segment.Snapshot.class);
        assertEqualsSnapshot(actual, expected);

        // Verify we can create a segment that looks like it's in the same state as the original.
        Segment hydrated = new Segment(segment.getId(), actual, 12345, splitQueue);
        assertEqualsSegment(hydrated, segment);
        assertNull(hydrated.getMin());
    }

    /** Verify we can load data from when segments had id == dataId so dataId was missing from the snapshot. */
    @Test
    @SuppressWarnings("unchecked")
    public void testOldSnapshotMigration() throws IOException {
        SplitQueue<Segment> splitQueue = mock(SplitQueue.class);

        String hllState = Base64.encodeBase64String(new HyperLogLog(Segment.HLL_LOG2M).getBytes());
        String json = "{\"version\":1,\"min\":null,\"adds\":1,\"bytesAdded\":20000," +
                "\"distinctAddsHll\":\"" + hllState + "\",\"deletes\":0,\"bytesUntilSplitCheckSize\":771," +
                "\"splitting\":true,\"splitTargetSize\":5500,\"splitTargetRemaining\":4266}";

        // Test decoding.
        Segment.Snapshot actual = JsonHelper.fromJson(json, Segment.Snapshot.class);

        // Verify we can create a segment that looks like it's in the same state as the original.
        UUID id = TimeUUIDs.newUUID();
        Segment hydrated = new Segment(id, actual, 12345, splitQueue);
        assertEquals(hydrated.getId(), id);
        assertEquals(hydrated.getDataId(), id);
    }

    @Test
    public void testSnapshotVersionCheck() throws Exception {
        SplitQueue<Segment> splitQueue = mock(SplitQueue.class);
        Segment segment = new Segment(TimeUUIDs.newUUID(), intBuffer(1), 12345, splitQueue);
        Segment.Snapshot expected = segment.snapshot();
        String json = JsonHelper.asJson(expected);

        // Hydrating from json should work.
        new Segment(segment.getId(), JsonHelper.fromJson(json, Segment.Snapshot.class), 12345, splitQueue);

        // Bump the persistent version.  Now hydrating from json should fail.
        json = json.replace("\"version\":1", "\"version\":2");
        try {
            new Segment(segment.getId(), JsonHelper.fromJson(json, Segment.Snapshot.class), 12345, splitQueue);
            fail();
        } catch (UnsupportedOperationException e) {
            // expected;
        }
    }

    private void assertEqualsSnapshot(Segment.Snapshot actual, Segment.Snapshot expected) {
        assertEquals(actual.min, expected.min);
        assertEquals(actual.adds, expected.adds);
        assertEquals(actual.bytesAdded, expected.bytesAdded);
        assertEquals(actual.distinctAddsHll, expected.distinctAddsHll);
        assertEquals(actual.deletes, expected.deletes);
        assertEquals(actual.bytesUntilSplitCheckSize, expected.bytesUntilSplitCheckSize);
        assertEquals(actual.splitting, expected.splitting);
        assertEquals(actual.splitTargetSize, expected.splitTargetSize);
        assertEquals(actual.splitTargetRemaining, expected.splitTargetRemaining);
    }

    private void assertEqualsSegment(Segment actual, Segment expected) {
        assertEquals(actual.getId(), expected.getId());
        assertEquals(actual.getDataId(), expected.getDataId());
        assertEquals(actual.isDeleted(), expected.isDeleted());
        assertEquals(actual.isSplitting(), expected.isSplitting());
        assertEquals(actual.getMin(), expected.getMin());
        assertEquals(actual.cardinality(), expected.cardinality());
        assertEquals(actual.distinctAdds(), expected.distinctAdds());
        assertEquals(actual.segmentSize(), expected.segmentSize());
        assertEquals(actual.recordSize(), expected.recordSize());
        assertEquals(actual.toString(), expected.toString());
    }

    private ByteBuffer intBuffer(int n) {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(n);
        buf.flip();
        return buf;
    }

    private ByteBuffer randomBuffer(int bufferSize) {
        byte[] buf = new byte[bufferSize];
        RANDOM.nextBytes(buf);
        return ByteBuffer.wrap(buf);
    }
}
