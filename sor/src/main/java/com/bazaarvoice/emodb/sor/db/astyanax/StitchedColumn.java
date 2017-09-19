package com.bazaarvoice.emodb.sor.db.astyanax;


import com.netflix.astyanax.Serializer;
import com.netflix.astyanax.model.AbstractColumnImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.serializers.UUIDSerializer;

import java.nio.ByteBuffer;
import java.util.UUID;

public class StitchedColumn extends AbstractColumnImpl<UUID> {
    private Column<DeltaKey> _oldColumn;
    private ByteBuffer _content;

    public StitchedColumn(Column<DeltaKey> oldColumn, ByteBuffer content) {
        super(oldColumn.getName().getChangeId());
        _oldColumn = oldColumn;
        _content = content;
    }

    public StitchedColumn(Column<DeltaKey> oldColumn) {
        super(oldColumn.getName().getChangeId());
        _oldColumn = oldColumn;
    }

    @Override
    public ByteBuffer getRawName() {
        return UUIDSerializer.get().toByteBuffer(getName());
    }

    @Override
    public long getTimestamp() {
        return _oldColumn.getTimestamp();
    }

    // content will be null if delta fit in a single block, as no stitching was performed
    @Override
    public <V> V getValue(Serializer<V> serializer) {
        return _content != null ? serializer.fromByteBuffer(_content) : _oldColumn.getValue(serializer);
    }

    @Override
    public int getTtl() {
        return _oldColumn.getTtl();
    }

    @Override
    public boolean hasValue() {
        return _oldColumn.hasValue();
    }
}