package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.db.test.DeltaClusteringKey;
import com.bazaarvoice.emodb.sor.delta.Delta;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The result of consolidating deltas and compaction records.
 */
public class PendingCompaction {
    private final UUID _compactionKey;
    private final Compaction _compaction;
    private final UUID _changeId;

    // Any keys before this marker are stale keys and not a part of this compaction
    @Nullable
    private final UUID _initialCutoffId;
    private final Delta _delta;
    private final Delta _startingDelta;
    private final List<DeltaClusteringKey> _keysToDelete;
    private final List<DeltaClusteringKey> _compactionKeysToDelete;
    private final List<Map.Entry<UUID, Delta>> _deltasToArchive;

    public PendingCompaction(UUID compactionKey, Compaction compaction, UUID changeId, UUID initialCutoffId, Delta delta, Delta startingDelta, List<DeltaClusteringKey> keysToDelete,
                             List<DeltaClusteringKey> compactionKeysToDelete, List<Map.Entry<UUID, Delta>> deltasToArchive) {
        _compactionKey = compactionKey;
        _compaction = compaction;
        _changeId = changeId;
        _initialCutoffId = initialCutoffId;
        _delta = delta;
        _keysToDelete = keysToDelete;
        _compactionKeysToDelete = compactionKeysToDelete;
        _deltasToArchive = checkNotNull(deltasToArchive, "deltasToArchive");
        _startingDelta = startingDelta;
    }

    public UUID getCompactionKey() {
        return _compactionKey;
    }

    public Compaction getCompaction() {
        return _compaction;
    }

    public UUID getChangeId() {
        return _changeId;
    }
    public UUID getInitialCutoffId() {
        return _initialCutoffId;
    }

    public Delta getDelta() {
        return _delta;
    }
    public Delta getStartingDelta() {
        return _startingDelta;
    }

    public List<DeltaClusteringKey> getKeysToDelete() {
        return _keysToDelete;
    }

    public List<DeltaClusteringKey> getCompactionKeysToDelete() {
        return _compactionKeysToDelete;
    }

    public List<Map.Entry<UUID, Delta>> getDeltasToArchive() {
        return _deltasToArchive;
    }
}
