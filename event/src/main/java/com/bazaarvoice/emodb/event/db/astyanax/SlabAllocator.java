package com.bazaarvoice.emodb.event.db.astyanax;

import com.google.common.collect.PeekingIterator;

public interface SlabAllocator {

    SlabAllocation allocate(String channel, int desiredCount, PeekingIterator<Integer> eventSizes);
}
