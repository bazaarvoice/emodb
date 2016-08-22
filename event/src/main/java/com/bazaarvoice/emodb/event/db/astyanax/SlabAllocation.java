package com.bazaarvoice.emodb.event.db.astyanax;

import java.nio.ByteBuffer;

public interface SlabAllocation {

    ByteBuffer getSlabId();

    int getOffset();

    int getLength();

    void release();
}
