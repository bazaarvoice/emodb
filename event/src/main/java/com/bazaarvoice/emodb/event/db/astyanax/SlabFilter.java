package com.bazaarvoice.emodb.event.db.astyanax;

import java.nio.ByteBuffer;

interface SlabFilter {
    /** Returns true to scan events in this slab, false to skip this slab and move on to the next. */
    boolean accept(ByteBuffer slabId, boolean open, ByteBuffer nextSlabId);
}
