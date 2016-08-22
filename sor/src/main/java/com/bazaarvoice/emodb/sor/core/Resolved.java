package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.eval.ConditionEvaluator;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.eval.DeltaEvaluator;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;

public class Resolved {

    /** Special marker content that indicates the value of the content has been deleted. */
    public static final Object UNDEFINED = DeltaEvaluator.UNDEFINED;

    @Nullable
    private final Object _content;
    private final MutableIntrinsics _intrinsics;
    private final UUID _lastCompactionCutoff;
    private final UUID _lastCompactedMutation;
    private final UUID _lastMutation;
    private final Set<UUID> _changesSinceLastCompaction;
    private final Set<UUID> _redundantChangesSinceLastCompaction;
    @Nullable
    private final Set<String> _lastTags;

    public Resolved(@Nullable Object content, MutableIntrinsics intrinsics, @Nullable UUID lastCompactionCutoff,
                    @Nullable UUID lastCompactedMutation, @Nullable UUID lastMutation,
                    Set<UUID> changesSinceLastCompaction, Set<UUID> redundantChangesSinceLastCompaction,
                    @Nullable Set<String> lastTags) {
        _content = content;
        _intrinsics = intrinsics;
        _lastCompactionCutoff = lastCompactionCutoff;
        _lastCompactedMutation = lastCompactedMutation;
        _lastMutation = lastMutation;
        _changesSinceLastCompaction = changesSinceLastCompaction;
        _redundantChangesSinceLastCompaction = redundantChangesSinceLastCompaction;
        _lastTags = lastTags;
    }

    public boolean isUndefined() {
        return _content == UNDEFINED;
    }

    @Nullable
    public Object getContent() {
        checkState(!isUndefined());
        return _content;
    }

    @Nullable
    public Set<String> getLastTags() {
        return _lastTags;
    }

    public Delta getConstant() {
        return isUndefined() ? Deltas.delete() : Deltas.literal(getContent());
    }

    public boolean matches(Condition condition) {
        return condition.visit(new ConditionEvaluator(_intrinsics), _content);
    }

    public MutableIntrinsics getIntrinsics() {
        return _intrinsics;
    }

    public UUID getLastMutation() {
        return _lastMutation;
    }

    public boolean isChangeDeltaPending(UUID changeId, long fullConsistencyTimestamp) {
        // Deltas older than the full consistency timestamp must be present, or else they don't exist. Also, deltas
        // that pre-date the last compaction cutoff are assumed to be deleted and in any case now irrelevant. (They're
        // rare but can happen after a manual compaction that uses a shorter TTL than the usual full consistency time.)
        return !(
                TimeUUIDs.getTimeMillis(changeId) <= fullConsistencyTimestamp ||
                        (_lastCompactionCutoff != null && TimeUUIDs.compare(changeId, _lastCompactionCutoff) <= 0) ||
                        _changesSinceLastCompaction.contains(changeId)
        );
    }

    public boolean isChangeDeltaRedundant(UUID changeId) {
        return _redundantChangesSinceLastCompaction.contains(changeId) ||
                (
                    _lastCompactedMutation != null &&
                    _lastCompactionCutoff != null &&
                    TimeUUIDs.compare(_lastCompactedMutation, changeId) < 0 &&
                    TimeUUIDs.compare(changeId, _lastCompactionCutoff) <= 0
                );
    }
}
