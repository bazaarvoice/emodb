package com.bazaarvoice.emodb.sor.db;

import com.google.common.collect.Lists;
import com.netflix.astyanax.serializers.StringSerializer;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class DeltaBlockingTest {

    private int _prefixLength = 4;
    private int _deltaBlockSize = 8;

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
            List<ByteBuffer> blocks = DAOUtils.getBlockedDeltas(ByteBuffer.wrap(encodedDelta.getBytes()), _prefixLength, _deltaBlockSize);
            UUID changeId = UUID.randomUUID();
            for (int i = 0; i < blocks.size(); i++) {
                rows.add(new TestRow(i, changeId, blocks.get(i)));
            }
        }
        Iterator<ByteBuffer> iterator = new ListDeltaIterator(rows.iterator(), false, _prefixLength);
        for (int i = 0; i < deltas.length; i++) {
            assertEquals(iterator.hasNext(), true);
            assertEquals(StringSerializer.get().fromByteBuffer(DAOUtils.removePrefix(iterator.next(), _prefixLength)), deltas[i]);
        }
        assertEquals(iterator.hasNext(), false);

        List<TestRow> reversedRows = Lists.reverse(rows);
        Iterator<ByteBuffer> reversedIterator = new ListDeltaIterator(reversedRows.iterator(), true, _prefixLength);

        for (int i = deltas.length - 1; i >= 0; i--) {
            assertEquals(reversedIterator.hasNext(), true);
            assertEquals(StringSerializer.get().fromByteBuffer(DAOUtils.removePrefix(reversedIterator.next(), _prefixLength)), deltas[i]);
        }
        assertEquals(reversedIterator.hasNext(), false);
    }

    @Test void testRemovePrefix() {
        String[] deltas = buildDeltas();
        String[] encodedDeltas = buildEncodedDeltas(deltas);
        List<TestRow> rows = Lists.newArrayListWithCapacity(deltas.length * 5); // lazy guess at future size
        for (int i = 0; i < encodedDeltas.length; i++) {
            ByteBuffer byteDelta = ByteBuffer.wrap((encodedDeltas[i].getBytes()));
            assertEquals(StringSerializer.get().fromByteBuffer(DAOUtils.removePrefix(byteDelta, _prefixLength)), deltas[i]);
        }
    }
}

class ListDeltaIterator extends DeltaIterator<TestRow, ByteBuffer> {
    public ListDeltaIterator(Iterator<TestRow> iterator, boolean reverse, int prefixLength) {
        super(iterator, reverse, prefixLength);
    }

    @Override
    protected ByteBuffer convertDelta(TestRow delta) {
        return delta.getContent();
    }

    @Override
    protected ByteBuffer convertDelta(TestRow delta, ByteBuffer content) {
        return content;
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
