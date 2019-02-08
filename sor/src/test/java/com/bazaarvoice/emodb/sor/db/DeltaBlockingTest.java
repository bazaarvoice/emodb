package com.bazaarvoice.emodb.sor.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.netflix.astyanax.serializers.StringSerializer;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DeltaBlockingTest {

    private int _prefixLength = 4;
    private int _deltaBlockSize = 8;
    private DAOUtils _daoUtils = new DAOUtils(_prefixLength, _deltaBlockSize);

    private String[] buildDeltas() {
        String[] deltas = new String[100000];
        for (int i = 0; i < deltas.length; i++) {
            deltas[i] = "D3:[]:0:{..,\"name\":\"bobåååååຄຄຄຄຄຄຄຄຄຄ" + i + "\"}";
        }
        return deltas;
    }

    private String[] buildEncodedDeltas(String[] deltas) {
        String[] encodedDeltas = new String[deltas.length];
        for (int i = 0; i < encodedDeltas.length; i++) {
            encodedDeltas[i] = "0000" + deltas[i];
        }

        return encodedDeltas;
    }

    @Test
    public void testBlockedReadWrite() {
        String[] deltas = buildDeltas();
        String[] encodedDeltas = buildEncodedDeltas(deltas);
        List<TestRow> rows = Lists.newArrayListWithCapacity(deltas.length * 5); // lazy guess at future size
        for (String encodedDelta : encodedDeltas) {
            List<ByteBuffer> blocks = _daoUtils.getDeltaBlocks(ByteBuffer.wrap(encodedDelta.getBytes()));
            UUID changeId = UUID.randomUUID();
            for (int i = 0; i < blocks.size(); i++) {
                rows.add(new TestRow(i, changeId, blocks.get(i)));
            }
        }
        Iterator<DeltaIterator.BlockedDelta> iterator = new ListDeltaIterator(rows.iterator(), false, _prefixLength);
        for (int i = 0; i < deltas.length; i++) {
            assertEquals(iterator.hasNext(), true);
            assertEquals(StringSerializer.get().fromByteBuffer(_daoUtils.skipPrefix(iterator.next().getContent())), deltas[i]);
        }
        assertEquals(iterator.hasNext(), false);

        List<TestRow> reversedRows = Lists.reverse(rows);
        Iterator<DeltaIterator.BlockedDelta> reversedIterator = new ListDeltaIterator(reversedRows.iterator(), true, _prefixLength);

        for (int i = deltas.length - 1; i >= 0; i--) {
            assertEquals(reversedIterator.hasNext(), true);
            assertEquals(StringSerializer.get().fromByteBuffer(_daoUtils.skipPrefix(reversedIterator.next().getContent())), deltas[i]);
        }
        assertEquals(reversedIterator.hasNext(), false);
    }

    @Test
    void testRemovePrefix() {
        String[] deltas = buildDeltas();
        String[] encodedDeltas = buildEncodedDeltas(deltas);
        List<TestRow> rows = Lists.newArrayListWithCapacity(deltas.length * 5); // lazy guess at future size
        for (int i = 0; i < encodedDeltas.length; i++) {
            ByteBuffer byteDelta = ByteBuffer.wrap((encodedDeltas[i].getBytes()));
            assertEquals(StringSerializer.get().fromByteBuffer(_daoUtils.skipPrefix(byteDelta)), deltas[i]);
        }
    }

    private String generateLargeDelta() throws IOException {
        Map<String, String> delta = new HashMap<>();
        for (int i = 0 ; i < 1000; i++) {
            delta.put("key" + i, "value" + i);
        }
        return "D3:[]:0:" + new ObjectMapper().writeValueAsString(delta);
    }

    @Test
    public void testLargeDelta() throws IOException {
        DAOUtils daoUtils = new DAOUtils(_prefixLength, 64 * 1024);
        String delta = generateLargeDelta();
        String encodedDelta = StringUtils.repeat('0', _prefixLength) + delta;
        List<ByteBuffer> blocks = daoUtils.getDeltaBlocks(ByteBuffer.wrap(encodedDelta.getBytes()));
        List<TestRow> rows = Lists.newArrayListWithCapacity(blocks.size());
        UUID changeId = UUID.randomUUID();
        for (int i = 0; i < blocks.size(); i++) {
            rows.add(new TestRow(i, changeId, blocks.get(i)));
        }

        Iterator<DeltaIterator.BlockedDelta> iterator = new ListDeltaIterator(rows.iterator(), false, _prefixLength);
        assertEquals(iterator.hasNext(), true);
        assertEquals(StringSerializer.get().fromByteBuffer(daoUtils.skipPrefix(iterator.next().getContent())), delta);
        assertEquals(iterator.hasNext(), false);

        List<TestRow> reversedRows = Lists.reverse(rows);
        Iterator<DeltaIterator.BlockedDelta> reversedIterator = new ListDeltaIterator(reversedRows.iterator(), true, _prefixLength);
        assertEquals(reversedIterator.hasNext(), true);
        assertEquals(StringSerializer.get().fromByteBuffer(daoUtils.skipPrefix(reversedIterator.next().getContent())), delta);
        assertEquals(reversedIterator.hasNext(), false);
    }

    @Test
    public void testFragmentedDelta() throws IOException {
        String delta = generateLargeDelta();
        String encodedDelta = StringUtils.repeat('0', _prefixLength) + delta;
        List<ByteBuffer> blocks = _daoUtils.getDeltaBlocks(ByteBuffer.wrap(encodedDelta.getBytes()));
        List<TestRow> rows = Lists.newArrayList();
        UUID changeId = UUID.randomUUID();
        for (int i = 0; i < blocks.size() - 1; i++) {
            rows.add(new TestRow(i, changeId, blocks.get(i)));
        }

        UUID secondDeltaUUID = UUID.randomUUID();

        List<ByteBuffer> secondDeltaBlocks = _daoUtils.getDeltaBlocks(ByteBuffer.wrap("0000D3:[]:0:{..,\"name\":\"bobåååååຄຄຄຄຄຄຄຄຄຄ\"}".getBytes()));

        for (int i = 0; i < secondDeltaBlocks.size(); i++) {
            rows.add(new TestRow(i, secondDeltaUUID, secondDeltaBlocks.get(i)));
        }

        Iterator<DeltaIterator.BlockedDelta> iterator = new ListDeltaIterator(rows.iterator(), false, _prefixLength);
        assertEquals(iterator.hasNext(), true);
        DeltaIterator.BlockedDelta secondBlockedDelta = iterator.next();
        assertEquals("000AD3:[]:0:{..,\"name\":\"bobåååååຄຄຄຄຄຄຄຄຄຄ\"}", StringSerializer.get().fromByteBuffer(secondBlockedDelta.getContent()));
        assertEquals(secondBlockedDelta.getNumBlocks(), secondDeltaBlocks.size());
        assertEquals(iterator.hasNext(), false);

        rows = Lists.newArrayList();

        for (int i = blocks.size() - 1; i >= 1; i--) {
            rows.add(new TestRow(i, changeId, blocks.get(i)));
        }

        for (int i = secondDeltaBlocks.size() - 1; i >= 0; i--) {
            rows.add(new TestRow(i, secondDeltaUUID, secondDeltaBlocks.get(i)));
        }

        Iterator<DeltaIterator.BlockedDelta> reversedIterator = new ListDeltaIterator(rows.iterator(), true, _prefixLength);
        assertEquals(reversedIterator.hasNext(), true);
        DeltaIterator.BlockedDelta reversedBlockedDelta = reversedIterator.next();
        assertEquals("000AD3:[]:0:{..,\"name\":\"bobåååååຄຄຄຄຄຄຄຄຄຄ\"}", StringSerializer.get().fromByteBuffer(reversedBlockedDelta.getContent()));
        assertEquals(reversedBlockedDelta.getNumBlocks(), secondDeltaBlocks.size());
        assertEquals(reversedIterator.hasNext(), false);
    }

    @Test
    public void testOverwrittenDelta() throws IOException {
        String delta = generateLargeDelta();
        String encodedDelta = StringUtils.repeat('0', _prefixLength) + delta;
        List<ByteBuffer> blocks = _daoUtils.getDeltaBlocks(ByteBuffer.wrap(encodedDelta.getBytes()));
        List<TestRow> rows = Lists.newArrayList();
        UUID changeId = UUID.randomUUID();
        for (int i = 0; i < blocks.size(); i++) {
            rows.add(new TestRow(i, changeId, blocks.get(i)));
        }

        int numExtraBlocks = 5;

        for (int i = blocks.size(); i < blocks.size() + numExtraBlocks; i++) {
            rows.add(new TestRow(i, changeId, ByteBuffer.wrap("this text should be ignored by the delta iterator!".getBytes())));
        }

        UUID secondDeltaUUID = UUID.randomUUID();

        List<ByteBuffer> secondDeltaBlocks = _daoUtils.getDeltaBlocks(ByteBuffer.wrap("0000D3:[]:0:{..,\"name\":\"bobåååååຄຄຄຄຄຄຄຄຄຄ\"}".getBytes()));

        for (int i = 0; i < secondDeltaBlocks.size(); i++) {
            rows.add(new TestRow(i, secondDeltaUUID, secondDeltaBlocks.get(i)));
        }

        Iterator<DeltaIterator.BlockedDelta> iterator = new ListDeltaIterator(rows.iterator(), false, _prefixLength);
        assertTrue(iterator.hasNext());
        DeltaIterator.BlockedDelta overwrittenDeltaWithExtraBlocks = iterator.next();
        assertEquals(StringSerializer.get().fromByteBuffer(_daoUtils.skipPrefix(overwrittenDeltaWithExtraBlocks.getContent())), delta);
        assertEquals(overwrittenDeltaWithExtraBlocks.getNumBlocks(), blocks.size() + numExtraBlocks);
        assertTrue(iterator.hasNext());

        DeltaIterator.BlockedDelta secondBlockedDelta = iterator.next();
        assertEquals("000AD3:[]:0:{..,\"name\":\"bobåååååຄຄຄຄຄຄຄຄຄຄ\"}", StringSerializer.get().fromByteBuffer(secondBlockedDelta.getContent()));
        assertEquals(secondBlockedDelta.getNumBlocks(), secondDeltaBlocks.size());
        assertFalse(iterator.hasNext());

        rows = Lists.newArrayList();

        for (int i = blocks.size() + numExtraBlocks - 1; i >= blocks.size(); i--) {
            rows.add(new TestRow(i, changeId, ByteBuffer.wrap("this text should be ignored by the delta iterator!".getBytes())));
        }

        for (int i = blocks.size() - 1; i >= 0; i--) {
            rows.add(new TestRow(i, changeId, blocks.get(i)));
        }

        for (int i = secondDeltaBlocks.size() - 1; i >= 0; i--) {
            rows.add(new TestRow(i, secondDeltaUUID, secondDeltaBlocks.get(i)));
        }

        Iterator<DeltaIterator.BlockedDelta> reversedIterator = new ListDeltaIterator(rows.iterator(), true, _prefixLength);
        assertTrue(reversedIterator.hasNext());
        DeltaIterator.BlockedDelta reversedOverwrittenDeltaWithExtraBlocks = reversedIterator.next();
        assertEquals(StringSerializer.get().fromByteBuffer(_daoUtils.skipPrefix(reversedOverwrittenDeltaWithExtraBlocks.getContent())), delta);
        assertEquals(reversedOverwrittenDeltaWithExtraBlocks.getNumBlocks(), blocks.size() + numExtraBlocks);
        assertTrue(reversedIterator.hasNext());
        DeltaIterator.BlockedDelta reversedBlockedDelta = reversedIterator.next();
        assertEquals("000AD3:[]:0:{..,\"name\":\"bobåååååຄຄຄຄຄຄຄຄຄຄ\"}", StringSerializer.get().fromByteBuffer(reversedBlockedDelta.getContent()));
        assertEquals(reversedBlockedDelta.getNumBlocks(), secondDeltaBlocks.size());
        assertFalse(reversedIterator.hasNext());
    }
}

class ListDeltaIterator extends DeltaIterator<TestRow, DeltaIterator.BlockedDelta> {
    public ListDeltaIterator(Iterator<TestRow> iterator, boolean reverse, int prefixLength) {
        super(iterator, reverse, prefixLength, "<row key placeholder>");
    }

    @Override
    protected BlockedDelta convertDelta(TestRow delta, BlockedDelta blockedDelta) {
        return blockedDelta;
    }

    @Override
    protected int getBlock(TestRow delta) {
        return delta.getBlock();
    }

    protected UUID getChangeId(TestRow delta) {
        return delta.getChangeId();
    }

    @Override
    protected ByteBuffer getValue(TestRow delta) {
        return delta.getContent();
    }
}

class TestRow {

    private int _block;
    private UUID _changeId;
    private ByteBuffer _content;

    public TestRow(int block, UUID changeId, ByteBuffer content) {
        _block = block;
        _changeId = changeId;
        _content = content;
    }

    public int getBlock() {
        return _block;
    }

    public UUID getChangeId() {
        return _changeId;
    }

    public ByteBuffer getContent() {
        return _content.duplicate();
    }

    public String toString() {
        return _block + " | " + _content;
    }
}
