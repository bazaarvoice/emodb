package com.bazaarvoice.emodb.event.db.astyanax;

import com.google.common.collect.Iterators;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;

public class ChannelAllocationStateTest {

    private static final Logger _log = LoggerFactory.getLogger(ChannelAllocationStateTest.class);

    @Test
    public void allocateOversizeEvent() {
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i=0; i<100; i++) {
            sizes.add(64);
        }
        sizes.add(Constants.MAX_EVENT_SIZE_IN_BYTES + 1);
        ChannelAllocationState channelAllocationState = new ChannelAllocationState();
        try {
            channelAllocationState.attachAndAllocate(mock(SlabRef.class), Iterators.peekingIterator(sizes.iterator()));
            assertTrue(false, "ERROR: No exception thrown when oversize event allocated");
        } catch (IllegalArgumentException e) {
            _log.info("SUCCESS: IllegalArgumentException thrown when oversize event allocated");
        } catch (Exception e) {
            assertTrue(false, "ERROR: " + e.getClass().getName() + " thrown when oversize event allocated");
        }
    }

    @Test
    public void allocateWholeSlabMaxNumberSmallEvents() {
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i=0; i<Constants.MAX_SLAB_SIZE; i++) {
            sizes.add(64);
        }
        ChannelAllocationState channelAllocationState = new ChannelAllocationState();
        SlabRef slabRef = mock(SlabRef.class);
        when(slabRef.addRef()).thenReturn(slabRef);
        try {
            channelAllocationState.attachAndAllocate(slabRef, Iterators.peekingIterator(sizes.iterator()));
            _log.info("SUCCESS:  allocated " + Constants.MAX_SLAB_SIZE + " 64-byte events");
        } catch (Exception e) {
            assertTrue(false, "ERROR: " + e.getClass().getName() + " thrown when max # (" + Constants.MAX_SLAB_SIZE + ") 64-byte events allocated");
        }
    }

    @Test
    public void allocateWholeSlabExactBytesMaxEvents() {
        int allocSize = Constants.MAX_SLAB_SIZE_IN_BYTES/Constants.MAX_SLAB_SIZE;
        int lastAllocExtraBytes = 0;
        if (Constants.MAX_SLAB_SIZE_IN_BYTES % Constants.MAX_SLAB_SIZE != 0) {
            lastAllocExtraBytes = Constants.MAX_SLAB_SIZE_IN_BYTES % Constants.MAX_SLAB_SIZE;
        }
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i=0; i<Constants.MAX_SLAB_SIZE; i++) {
            if (i == Constants.MAX_SLAB_SIZE-1) {
                sizes.add(allocSize+lastAllocExtraBytes);
            } else {
                sizes.add(allocSize);
            }
        }
        ChannelAllocationState channelAllocationState = new ChannelAllocationState();
        SlabRef slabRef = mock(SlabRef.class);
        when(slabRef.addRef()).thenReturn(slabRef);
        try {
            channelAllocationState.attachAndAllocate(slabRef,  Iterators.peekingIterator(sizes.iterator()));
            _log.info("SUCCESS:  allocated " + Constants.MAX_SLAB_SIZE + " events that exactly filled the slab (" + Constants.MAX_SLAB_SIZE_IN_MEGABYTES + " MB)");
        } catch (Exception e) {
            assertTrue(false, "ERROR: " + e.getClass().getName() + " thrown when max # (" + Constants.MAX_SLAB_SIZE + ") events that exactly filled slab allocated");
        }
    }

    @Test
    public void allocateWholeSlabExactBytesLessThanMaxEvents() {
        int allocSize = Constants.MAX_SLAB_SIZE_IN_BYTES/(Constants.MAX_SLAB_SIZE/2);
        int lastAllocExtraBytes = 0;
        if (Constants.MAX_SLAB_SIZE_IN_BYTES % (Constants.MAX_SLAB_SIZE/2) != 0) {
            lastAllocExtraBytes = Constants.MAX_SLAB_SIZE_IN_BYTES % (Constants.MAX_SLAB_SIZE/2);
        }
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i=0; i<Constants.MAX_SLAB_SIZE/2; i++) {
            if (i == Constants.MAX_SLAB_SIZE/2-1) {
                sizes.add(allocSize+lastAllocExtraBytes);
            } else {
                sizes.add(allocSize);
            }
        }
        ChannelAllocationState channelAllocationState = new ChannelAllocationState();
        SlabRef slabRef = mock(SlabRef.class);
        when(slabRef.addRef()).thenReturn(slabRef);
        try {
            channelAllocationState.attachAndAllocate(slabRef, Iterators.peekingIterator(sizes.iterator()));
            _log.info("SUCCESS:  allocated " + Constants.MAX_SLAB_SIZE/2 + " events that exactly filled the slab (" + Constants.MAX_SLAB_SIZE_IN_MEGABYTES + " MB)");
        } catch (Exception e) {
            assertTrue(false, "ERROR: " + e.getClass().getName() + " thrown when 1/2 max # (" + Constants.MAX_SLAB_SIZE/2 + ") events that exactly filled slab allocated");
        }
    }

    @Test
    public void allocateLessThanMaxBytesLessThanMaxEvents() {
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i=0; i<Constants.MAX_SLAB_SIZE-1; i++) {
            sizes.add(64);
        }
        ChannelAllocationState channelAllocationState = new ChannelAllocationState();
        SlabRef slabRef = mock(SlabRef.class);
        when(slabRef.addRef()).thenReturn(slabRef);
        try {
            channelAllocationState.attachAndAllocate(slabRef, Iterators.peekingIterator(sizes.iterator()));
            _log.info("SUCCESS:  allocated " + (Constants.MAX_SLAB_SIZE-1) + " 64-byte events");
        } catch (Exception e) {
            assertTrue(false, "ERROR: " + e.getClass().getName() + " thrown when max # -1 (" + (Constants.MAX_SLAB_SIZE-1) + ") 64-byte events allocated");
        }
    }

    @Test
    public void allocateSlabInMultipleCalls() {
        List<Integer> sizes = new ArrayList<Integer>();
        for (int i=0; i<Constants.MAX_SLAB_SIZE/2; i++) {
            sizes.add(64);
        }
        ChannelAllocationState channelAllocationState = new ChannelAllocationState();
        SlabRef slabRef = mock(SlabRef.class);
        when(slabRef.addRef()).thenReturn(slabRef);
        try {
            channelAllocationState.attachAndAllocate(slabRef,  Iterators.peekingIterator(sizes.iterator()));
            _log.info("SUCCESS:  allocated " + Constants.MAX_SLAB_SIZE/2 + " 64-byte events");
        } catch (Exception e) {
            assertTrue(false, "ERROR: " + e.getClass().getName() + " thrown when first max/2 # (" + Constants.MAX_SLAB_SIZE/2 + ") 64-byte events allocated");
        }
        try {
            channelAllocationState.allocate(Iterators.peekingIterator(sizes.iterator()));
            _log.info("SUCCESS:  allocated " + Constants.MAX_SLAB_SIZE/2 + " 64-byte events");
        } catch (Exception e) {
            assertTrue(false, "ERROR: " + e.getClass().getName() + " thrown when second max/2 # (" + Constants.MAX_SLAB_SIZE/2 + ") 64-byte events allocated");
        }
    }
}
