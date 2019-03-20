package com.bazaarvoice.emodb.event.db.astyanax;

import com.google.common.collect.PeekingIterator;
import org.apache.commons.lang3.tuple.Pair;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class ChannelAllocationState {
    private final Object _slabCreationLock = new Object();
    private SlabRef _slab;
    private int _slabConsumed;
    private long _slabExpiresAt;
    private int _slabBytesConsumed;

    /** Per-channel lock on creating new shared slabs. */
    public Object getSlabCreationLock() {
        return _slabCreationLock;
    }

    public synchronized void rotateIfNecessary() {
        if (isAttached() && _slabExpiresAt <= System.currentTimeMillis()) {
            detach().release();
        }
    }

    private synchronized boolean isAttached() {
        return _slab != null;
    }

    /** Attaches a slab and allocates from it in a single atomic operation. */
    public synchronized SlabAllocation attachAndAllocate(SlabRef slab, PeekingIterator<Integer> eventSizes) {
        attach(slab);
        return allocate(eventSizes);
    }

    /** Attaches a new slab to the channel with the specified capacity. */
    public synchronized void attach(SlabRef slab) {
        // Assume ownership of the caller's ref.  No need to call slab.addRef().
        checkState(!isAttached());

        _slab = slab;
        _slabConsumed = 0;
        _slabBytesConsumed = 0;
        _slabExpiresAt = System.currentTimeMillis() + Constants.SLAB_ROTATE_TTL.toMillis();
    }

    /** Detaches a slab from the channel and returns it to the caller to dispose of. */
    public synchronized SlabRef detach() {
        if (!isAttached()) {
            return null;
        }

        // Pass ownership of the slab ref to the caller.  No need to call slab.release().
        SlabRef slab = _slab;
        _slab = null;
        _slabExpiresAt = 0;
        return slab;
    }

    public synchronized SlabAllocation allocate(PeekingIterator<Integer> eventSizes) {
        checkArgument(eventSizes.hasNext());

        if (!isAttached()) {
            return null;
        }

        int remaining = Constants.MAX_SLAB_SIZE - _slabConsumed;

        Pair<Integer, Integer> countAndBytesConsumed = DefaultSlabAllocator.defaultAllocationCount(_slabConsumed, _slabBytesConsumed, eventSizes);

        int offsetForNewAllocation = _slabConsumed;

        _slabConsumed += countAndBytesConsumed.getLeft();
        _slabBytesConsumed += countAndBytesConsumed.getRight();

        // Check for case where no more slots could be allocated because slab is full either because
        // the max # of slots is consumed or the max # of bytes is consumed
        if (countAndBytesConsumed.getLeft() == 0) {

            detach().release();
            return null;

        } else {

            if (countAndBytesConsumed.getLeft() < remaining) {

                // All events fit in current slab, leave it attached
                return new DefaultSlabAllocation(_slab.addRef(), offsetForNewAllocation, countAndBytesConsumed.getLeft());

            } else {

                // Whatever is left of this slab is consumed. Return the rest of the slab. Detach from it so we'll allocate a new one next time.
                return new DefaultSlabAllocation(detach(), offsetForNewAllocation, countAndBytesConsumed.getLeft());

            }
        }
    }
}
