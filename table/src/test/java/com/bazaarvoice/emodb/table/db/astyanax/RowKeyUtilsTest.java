package com.bazaarvoice.emodb.table.db.astyanax;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.util.ByteBufferRangeImpl;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import java.util.Random;

import static com.bazaarvoice.emodb.table.db.astyanax.RowKeyUtils.LEGACY_SHARDS_LOG2;
import static com.bazaarvoice.emodb.table.db.astyanax.RowKeyUtils.computeShardsLog2;
import static org.testng.Assert.assertEquals;

public class RowKeyUtilsTest {
    private static final int DEFAULT_SHARDS_LOG2 = computeShardsLog2(16, "test");
    private static final long UUID = 0x123456789abcdef1L;
    private static final String KEY = "hello-world\u0106";

    @Test
    public void testRowKey() {
        ByteBuffer rowKey = RowKeyUtils.getRowKey(UUID, DEFAULT_SHARDS_LOG2, KEY);
        assertRowKeyLayout(rowKey, 0x61, UUID, KEY);
    }

    @Test
    public void testLegacyRowKey() {
        ByteBuffer rowKey = RowKeyUtils.getRowKey(UUID, LEGACY_SHARDS_LOG2, KEY);
        assertRowKeyLayout(rowKey, 0x69, UUID, KEY);
    }

    @Test
    public void testRowKeyRaw() {
        // The natural shardId is 0x69 (105) so test w/a different shardId (0xcc) to verify it's accepted.
        ByteBuffer rowKey = RowKeyUtils.getRowKeyRaw(0xcc, 0x123456789abcdef1L, KEY);
        assertRowKeyLayout(rowKey, 0xcc, UUID, KEY);
    }

    @Test
    public void testRowKeyRawMin() {
        ByteBuffer rowKey = RowKeyUtils.getRowKeyRaw(0, 0, "");
        assertRowKeyLayout(rowKey, 0, 0, "");

        byte[] bytes = ByteBufferUtil.getArray(rowKey);
        assertEquals(bytes.length, 9);
        for (byte b : bytes) {
            assertEquals(b & 0xff, 0);
        }
    }

    @Test
    public void testRowKeyRawMax() {
        ByteBuffer rowKey = RowKeyUtils.getRowKeyRaw(0xff, -1L, "");
        assertRowKeyLayout(rowKey, 0xff, -1L, "");

        byte[] bytes = ByteBufferUtil.getArray(rowKey);
        assertEquals(bytes.length, 9);
        for (byte b : bytes) {
            assertEquals(b & 0xff, 0xff);
        }
    }

    @Test
    public void testShardId() {
        assertEquals(RowKeyUtils.getShardId(RowKeyUtils.getRowKey(UUID, DEFAULT_SHARDS_LOG2, KEY)), 0x61);
        assertEquals(RowKeyUtils.getShardId(RowKeyUtils.getRowKeyRaw(0xcc, 0x123456789abcdef1L, KEY)), 0xcc);
    }

    @Test
    public void testLegacyShardId() {
        assertEquals(RowKeyUtils.getShardId(RowKeyUtils.getRowKey(UUID, LEGACY_SHARDS_LOG2, KEY)), 0x69);
    }

    @Test
    public void testTableUuid() {
        assertEquals(RowKeyUtils.getTableUuid(RowKeyUtils.getRowKey(UUID, DEFAULT_SHARDS_LOG2, KEY)), UUID);
    }

    @Test
    public void testLegacyTableUuid() {
        assertEquals(RowKeyUtils.getTableUuid(RowKeyUtils.getRowKey(UUID, LEGACY_SHARDS_LOG2, KEY)), UUID);
    }

    @Test
    public void testContentKey() {
        assertEquals(RowKeyUtils.getContentKey(RowKeyUtils.getRowKey(UUID, DEFAULT_SHARDS_LOG2, KEY)), KEY);
    }

    @Test
    public void testLegacyContentKey() {
        assertEquals(RowKeyUtils.getContentKey(RowKeyUtils.getRowKey(UUID, LEGACY_SHARDS_LOG2, KEY)), KEY);
    }

    @Test
    public void testScanIterator() {
        List<ByteBufferRange> actual = ImmutableList.copyOf(RowKeyUtils.scanIterator(UUID, DEFAULT_SHARDS_LOG2, null));

        List<ByteBufferRange> expected = Lists.newArrayList();
        // Start at 0x01 because the least significant 4 bits of UUID is 1.
        for (int i = 0x01; i < 256; i += 16) {
            expected.add(new ByteBufferRangeImpl(
                    RowKeyUtils.getRowKeyRaw(i, UUID, ""),
                    RowKeyUtils.getRowKeyRaw(i, UUID + 1, ""),
                    -1, false));
        }

        assertRangeListsEqual(actual, expected);
    }

    @Test
    public void testLegacyScanIterator() {
        List<ByteBufferRange> actual = ImmutableList.copyOf(RowKeyUtils.scanIterator(UUID, LEGACY_SHARDS_LOG2, null));

        List<ByteBufferRange> expected = Lists.newArrayList();
        for (int i = 0; i < 256; i++) {
            expected.add(new ByteBufferRangeImpl(
                    RowKeyUtils.getRowKeyRaw(i, UUID, ""),
                    RowKeyUtils.getRowKeyRaw(i, UUID + 1, ""),
                    -1, false));
        }

        assertRangeListsEqual(actual, expected);
    }

    @Test
    public void testScanIteratorFrom() {
        List<ByteBufferRange> actual = ImmutableList.copyOf(RowKeyUtils.scanIterator(UUID, DEFAULT_SHARDS_LOG2, KEY));

        List<ByteBufferRange> expected = Lists.newArrayList();
        for (int i = 0x61; i < 256; i += 16) {
            expected.add(new ByteBufferRangeImpl(
                    RowKeyUtils.getRowKeyRaw(i, UUID, i == 0x61 ? KEY : ""),
                    RowKeyUtils.getRowKeyRaw(i, UUID + 1, ""),
                    -1, false));
        }

        assertRangeListsEqual(actual, expected);
    }

    @Test
    public void testLegacyScanIteratorFrom() {
        List<ByteBufferRange> actual = ImmutableList.copyOf(RowKeyUtils.scanIterator(UUID, LEGACY_SHARDS_LOG2, KEY));

        List<ByteBufferRange> expected = Lists.newArrayList();
        for (int i = 0x69; i < 256; i++) {
            expected.add(new ByteBufferRangeImpl(
                    RowKeyUtils.getRowKeyRaw(i, UUID, i == 0x69 ? KEY : ""),
                    RowKeyUtils.getRowKeyRaw(i, UUID + 1, ""),
                    -1, false));
        }

        assertRangeListsEqual(actual, expected);
    }

    @Test
    public void testSplitRange() {
        ByteBufferRange range = new ByteBufferRangeImpl(
                RowKeyUtils.getRowKeyRaw(0xd1, UUID, KEY),
                RowKeyUtils.getRowKeyRaw(0xd1, UUID + 1, ""),
                -1, false);
        ByteBufferRange actual = RowKeyUtils.getSplitRange(UUID, DEFAULT_SHARDS_LOG2, range, null, "split-description");

        assertRangesEqual(actual, range);
    }

    @Test
    public void testLegacySplitRange() {
        ByteBufferRange range = new ByteBufferRangeImpl(
                RowKeyUtils.getRowKeyRaw(0xd1, UUID, KEY),
                RowKeyUtils.getRowKeyRaw(0xd1, UUID + 1, ""),
                -1, false);
        ByteBufferRange actual = RowKeyUtils.getSplitRange(UUID, LEGACY_SHARDS_LOG2, range, null, "split-description");

        assertRangesEqual(actual, range);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testSplitRangeInvalidOrder() {
        ByteBufferRange range = new ByteBufferRangeImpl(
                RowKeyUtils.getRowKeyRaw(0xd1, UUID + 1, KEY),
                RowKeyUtils.getRowKeyRaw(0xd1, UUID, ""),
                -1, false);
        RowKeyUtils.getSplitRange(UUID, DEFAULT_SHARDS_LOG2, range, null, "split-description");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testSplitRangeInvalidShardId() {
        ByteBufferRange range = new ByteBufferRangeImpl(
                RowKeyUtils.getRowKeyRaw(0xd2, UUID, KEY),
                RowKeyUtils.getRowKeyRaw(0xd2, UUID + 1, ""),
                -1, false);
        RowKeyUtils.getSplitRange(UUID, DEFAULT_SHARDS_LOG2, range, null, "split-description");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLegacySplitRangeInvalidOrder() {
        ByteBufferRange range = new ByteBufferRangeImpl(
                RowKeyUtils.getRowKeyRaw(0xd1, UUID + 1, KEY),
                RowKeyUtils.getRowKeyRaw(0xd1, UUID, ""),
                -1, false);
        RowKeyUtils.getSplitRange(UUID, LEGACY_SHARDS_LOG2, range, null, "split-description");
    }

    @Test
    public void testSplitRangeFrom() {
        ByteBufferRange range = new ByteBufferRangeImpl(
                RowKeyUtils.getRowKeyRaw(0x61, UUID, "abc"),
                RowKeyUtils.getRowKeyRaw(0x61, UUID + 1, ""),
                -1, false);
        ByteBufferRange expected = new ByteBufferRangeImpl(
                RowKeyUtils.getRowKeyRaw(0x61, UUID, KEY),
                RowKeyUtils.getRowKeyRaw(0x61, UUID + 1, ""),
                -1, false);
        ByteBufferRange actual = RowKeyUtils.getSplitRange(UUID, DEFAULT_SHARDS_LOG2, range, KEY, "split-description");

        assertRangesEqual(actual, expected);
    }

    @Test
    public void testLegacySplitRangeFrom() {
        ByteBufferRange range = new ByteBufferRangeImpl(
                RowKeyUtils.getRowKeyRaw(0x69, UUID, "abc"),
                RowKeyUtils.getRowKeyRaw(0x69, UUID + 1, ""),
                -1, false);
        ByteBufferRange expected = new ByteBufferRangeImpl(
                RowKeyUtils.getRowKeyRaw(0x69, UUID, KEY),
                RowKeyUtils.getRowKeyRaw(0x69, UUID + 1, ""),
                -1, false);
        ByteBufferRange actual = RowKeyUtils.getSplitRange(UUID, LEGACY_SHARDS_LOG2, range, KEY, "split-description");

        assertRangesEqual(actual, expected);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testSplitRangeInvalidFrom() {
        ByteBufferRange range = new ByteBufferRangeImpl(
                RowKeyUtils.getRowKeyRaw(0xd1, UUID, "abc"),
                RowKeyUtils.getRowKeyRaw(0xd1, UUID + 1, ""),
                -1, false);
        RowKeyUtils.getSplitRange(UUID, DEFAULT_SHARDS_LOG2, range, KEY, "split-description");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testLegacySplitRangeInvalidFrom() {
        ByteBufferRange range = new ByteBufferRangeImpl(
                RowKeyUtils.getRowKeyRaw(0xd1, UUID, "abc"),
                RowKeyUtils.getRowKeyRaw(0xd1, UUID + 1, ""),
                -1, false);
        RowKeyUtils.getSplitRange(UUID, LEGACY_SHARDS_LOG2, range, KEY, "split-description");
    }

    @Test
    public void testComputeShardsLog2_16() {
        assertEquals(RowKeyUtils.computeShardsLog2(16, "uuid"), DEFAULT_SHARDS_LOG2);
    }

    @Test
    public void testComputeShardsLog2_256() {
        assertEquals(RowKeyUtils.computeShardsLog2(256, "uuid"), LEGACY_SHARDS_LOG2);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testComputeShardsLog2_NotPower2() {
        RowKeyUtils.computeShardsLog2(15, "uuid");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testComputeShardsLog2_Low() {
        RowKeyUtils.computeShardsLog2(0, "uuid");
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testComputeShardsLog2_High() {
        RowKeyUtils.computeShardsLog2(512, "uuid");
    }

    /**
     * Verify that a large set of tables will be spread around all distinct key prefixes, ensuring a roughly equal
     * distribution of data around a large set of Cassandra servers.  For example, consider a hypothetical cluster
     * of 256 Cassandra servers w/replication factor=1 and tokens of 00, 01, 02, 03, ... FE, FF.  The row keys
     * should distribute such that data is distributed among all 256 servers, not some smaller subset.  In practice,
     * this test verifies that RowKeyUtils.mix() uses all bits, doesn't leave some always 0 or always 1.
     */
    @Test
    public void testFirstByteDistribution() {
        BitSet distinctFirstBytes = new BitSet();
        Random random = new Random();
        for (int i = 0; i < 1000000 && distinctFirstBytes.cardinality() < 256; i++) {
            long tableUuid = random.nextLong();
            ByteBuffer rowKey = RowKeyUtils.getRowKey(tableUuid, DEFAULT_SHARDS_LOG2, "test-key");
            distinctFirstBytes.set(RowKeyUtils.getShardId(rowKey));
        }
        assertEquals(distinctFirstBytes.cardinality(), 256);
    }

    private void assertRowKeyLayout(ByteBuffer actual, int shardId, long tableUuid, String contentKey) {
        byte[] bytes = ByteBufferUtil.getArray(actual);
        int idx = 0;
        // shard id first
        assertEquals(bytes[idx++] & 0xff, shardId);
        // table id next, big-endian
        for (int i = 56; i >= 0; i -= 8) {
            assertEquals(bytes[idx++] & 0xff, (tableUuid >>> i) & 0xff);
        }
        // content key next, UTF-8
        assertEquals(new String(bytes, idx, bytes.length - idx, Charsets.UTF_8), contentKey);
    }

    private void assertRangeListsEqual(List<ByteBufferRange> actual, List<ByteBufferRange> expected) {
        assertEquals(actual.size(), expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertRangesEqual(actual.get(i), expected.get(i));
        }
    }

    private void assertRangesEqual(ByteBufferRange actual, ByteBufferRange expected) {
        assertBuffersEqual(actual.getStart(), expected.getStart());
        assertBuffersEqual(actual.getEnd(), expected.getEnd());
        assertEquals(actual.getLimit(), expected.getLimit());
        assertEquals(actual.isReversed(), expected.isReversed());
    }

    private void assertBuffersEqual(ByteBuffer actual, ByteBuffer expected) {
        assertEquals(actual, expected, "Mismatch: " +
                "expected=" + (expected != null ? ByteBufferUtil.bytesToHex(expected) : "null") + ", " +
                "actual=" + (actual != null ? ByteBufferUtil.bytesToHex(actual) : "null"));
    }
}
