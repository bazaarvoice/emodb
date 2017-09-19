package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.sor.db.DeltaIterator;
import com.netflix.astyanax.model.Column;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.UUID;

public class AstyanaxDeltaIterator extends DeltaIterator<Column<DeltaKey>, Column<UUID>> {
    public AstyanaxDeltaIterator(Iterator<Column<DeltaKey>> iterator, boolean reversed, int prefixLength) {
        super(iterator, reversed, prefixLength);
    }

    @Override
    protected Column<UUID> convertDelta(Column<DeltaKey> delta, ByteBuffer content) {
        return new StitchedColumn(delta, content);
    }

    @Override
    protected Column<UUID> convertDelta(Column<DeltaKey> delta) {
        return new StitchedColumn(delta);
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