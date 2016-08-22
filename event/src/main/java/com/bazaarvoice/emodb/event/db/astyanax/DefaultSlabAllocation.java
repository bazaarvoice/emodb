package com.bazaarvoice.emodb.event.db.astyanax;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultSlabAllocation implements SlabAllocation {
    private final SlabRef _slab;
    private final int _offset;
    private final int _length;

    public DefaultSlabAllocation(SlabRef slab, int offset, int length) {
        checkArgument(offset >= 0);
        checkArgument(length > 0);
        _slab = checkNotNull(slab);
        _offset = offset;
        _length = length;
    }

    @Override
    public ByteBuffer getSlabId() {
        return _slab.getSlabId();
    }

    @Override
    public int getOffset() {
        return _offset;
    }

    @Override
    public int getLength() {
        return _length;
    }

    @Override
    public void release() {
        _slab.release();
    }
}
