package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.sor.db.DeltaIterator;
import com.bazaarvoice.emodb.sor.db.test.DeltaClusteringKey;
import com.netflix.astyanax.model.Column;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.UUID;

public class AstyanaxDeltaIterator extends DeltaIterator<Column<DeltaKey>, StitchedColumn> {
    public AstyanaxDeltaIterator(Iterator<Column<DeltaKey>> iterator, boolean reversed, int prefixLength, String rowKey) {
        super(iterator, reversed, prefixLength, rowKey);
    }

    @Override
    protected StitchedColumn convertDelta(Column<DeltaKey> delta, BlockedDelta blockedDelta) {
        return new StitchedColumn(delta, blockedDelta.getContent(), blockedDelta.getNumBlocks());
    }

    @Override
    protected int getBlock(Column<DeltaKey> column) {
        return column.getName().getBlock();
    }

    @Override
    protected UUID getChangeId(Column<DeltaKey> column) {
        return column.getName().getChangeId();
    }

    @Override
    protected ByteBuffer getValue(Column<DeltaKey> column) {
        return column.getByteBufferValue();
    }
}