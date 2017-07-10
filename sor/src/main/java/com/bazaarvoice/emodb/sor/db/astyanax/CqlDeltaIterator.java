
package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.sor.db.DeltaIterator;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.UUID;

/*
* CQL implementation of DeltaIterator.
* Used to stitch blocked deltas back together on read.
 */
public class CqlDeltaIterator extends DeltaIterator<Row, Row> {

    private final ProtocolVersion _protocolVersion;
    private final CodecRegistry _codecRegistry;
    private final int _blockIndex;
    private final int _changeIdIndex;
    private final int _contentIndex;

    public CqlDeltaIterator(Iterator<Row> iterator, final int blockIndex, final int changeIdIndex, final int contentIndex, boolean reversed, int prefixLength,
                            ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
        super(iterator, reversed, prefixLength);
        _blockIndex = blockIndex;
        _changeIdIndex = changeIdIndex;
        _contentIndex = contentIndex;
        _protocolVersion = protocolVersion;
        _codecRegistry = codecRegistry;
    }

    @Override
    protected Row convertDelta(Row row) {
        return row;
    }

    @Override
    protected Row convertDelta(Row row, ByteBuffer content) {
        return new StitchedRow(_protocolVersion, _codecRegistry, row, content, _contentIndex);
    }

    @Override
    protected int getBlock(Row row) {
        return row.getInt(_blockIndex);
    }

    @Override
    protected UUID getChangeId(Row row) {
        return row.getUUID(_changeIdIndex);
    }

    @Override
    protected ByteBuffer getValue(Row row) {
        return row.getBytesUnsafe(_contentIndex);
    }
}