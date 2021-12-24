package com.bazaarvoice.emodb.sor.core;

import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/** The result of resolving a sequence of deltas in the presence of the compaction algorithm. */
public class Expanded {
    private final Resolved _resolved;
    private final PendingCompaction _pendingCompaction;
    private final int _numPersistentDeltas;
    private final long _numDeletedDeltas;

    public Expanded(Resolved resolved, @Nullable PendingCompaction pendingCompaction,
                    int numPersistentDeltas, long numDeletedDeltas) {
        _resolved = requireNonNull(resolved, "resolved");
        _pendingCompaction = pendingCompaction;
        _numPersistentDeltas = numPersistentDeltas;
        _numDeletedDeltas = numDeletedDeltas;
    }

    public Resolved getResolved() {
        return _resolved;
    }

    public PendingCompaction getPendingCompaction() {
        return _pendingCompaction;
    }

    public int getNumPersistentDeltas() {
        return _numPersistentDeltas;
    }

    public long getNumDeletedDeltas() {
        return _numDeletedDeltas;
    }
}
