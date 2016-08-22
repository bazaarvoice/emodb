package com.bazaarvoice.emodb.event.db.astyanax;

public interface SlabAllocator {

    SlabAllocation allocate(String channel, int desiredCount);
}
