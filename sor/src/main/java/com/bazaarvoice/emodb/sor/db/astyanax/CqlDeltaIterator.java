package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.sor.db.DeltaIterator;
import com.datastax.driver.core.Row;

import java.nio.ByteBuffer;
import java.util.Iterator;


public class CqlDeltaIterator extends DeltaIterator<Row, Row> {

    private final int _blockIndex;
    private final int _contentIndex;

    public CqlDeltaIterator(Iterator<Row> iterator, final int blockIndex, final int contentIndex) {
        super(iterator);
        _blockIndex = blockIndex;
        _contentIndex = contentIndex;
    }

    @Override
    protected Row convertDelta(Row row) {
        return row;
    }

    @Override
    protected Row convertDelta(Row row, ByteBuffer content) {
        return new StitchedRow(row, content, _contentIndex);
    }

    @Override
    protected int getBlock(Row row) {
        return row.getInt(_blockIndex);
    }

    @Override
    protected ByteBuffer getValue(Row row) {
        return row.getBytesUnsafe(_contentIndex);
    }
}
