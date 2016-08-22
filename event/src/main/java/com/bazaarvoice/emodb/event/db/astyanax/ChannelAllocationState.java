package com.bazaarvoice.emodb.event.db.astyanax;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class ChannelAllocationState {
    private final Object _slabCreationLock = new Object();
    private SlabRef _slab;
    private int _slabConsumed;
    private long _slabExpiresAt;

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
    public synchronized SlabAllocation attachAndAllocate(SlabRef slab, int desired) {
        attach(slab);
        return allocate(desired);
    }

    /** Attaches a new slab to the channel with the specified capacity. */
    public synchronized void attach(SlabRef slab) {
        // Assume ownership of the caller's ref.  No need to call slab.addRef().
        checkState(!isAttached());

        _slab = slab;
        _slabConsumed = 0;
        _slabExpiresAt = System.currentTimeMillis() + Constants.SLAB_ROTATE_TTL.getMillis();
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

    public synchronized SlabAllocation allocate(int desired) {
        checkArgument(desired > 0);

        if (!isAttached()) {
            return null;
        }

        int remaining = Constants.MAX_SLAB_SIZE - _slabConsumed;
        if (desired < remaining) {
            // Return a portion of the slab.
            DefaultSlabAllocation allocation = new DefaultSlabAllocation(_slab.addRef(), _slabConsumed, desired);
            _slabConsumed += desired;
            return allocation;
        } else {
            // Return the rest of the slab.  Detach from it so we'll allocate a new one next time.
            return new DefaultSlabAllocation(detach(), _slabConsumed, remaining);
        }
    }
}
