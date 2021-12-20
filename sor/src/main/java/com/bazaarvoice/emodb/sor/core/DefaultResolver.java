package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.uuid.UUIDs;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.eval.DeltaEvaluator;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.util.Set;
import java.util.UUID;

public class DefaultResolver implements Resolver {

    private static final HashFunction HASH_FUNCTION = Hashing.md5();
    private static final HashCode HASH_ZERO = HashCode.fromBytes(new byte[16]);  // MD5 hashes are 16 bytes

    private Object _content;
    private final MutableIntrinsics _intrinsics;
    private UUID _compactionCutoffId;
    private String _compactionCutoffSignature;
    private UUID _lastCompactedMutationId;
    private UUID _lastMutationId;
    private Set<UUID> _changeIds;
    private Set<UUID> _redundantChangeIds;
    private Set<String> _lastAppliedTags = ImmutableSet.of();

    public DefaultResolver(MutableIntrinsics intrinsics) {
        _content = Resolved.UNDEFINED;
        _intrinsics = intrinsics;
        _intrinsics.setDeleted(true);
        _intrinsics.setSignature(HASH_ZERO);
        _changeIds = Sets.newHashSet();
        _redundantChangeIds = Sets.newHashSet();
    }

    public DefaultResolver(MutableIntrinsics intrinsics, Compaction compaction) {
        this(intrinsics);
        _intrinsics.setVersion(compaction.getCount());
        _intrinsics.setFirstUpdateAt(compaction.getFirst());
        _intrinsics.setLastMutateAt(compaction.getFirst());
        _intrinsics.setLastUpdateAt(compaction.getFirst());
        _compactionCutoffId = compaction.getCutoff();
        _compactionCutoffSignature = compaction.getCutoffSignature();
        _lastCompactedMutationId = compaction.getLastMutation();
        _lastMutationId = _lastCompactedMutationId;
        if (compaction.hasCompactedDelta()) {
            // We have compacted delta in this compaction. No cutoff delta was mutated as a part of this compaction.
            _content = DeltaEvaluator.eval(compaction.getCompactedDelta(), _content, _intrinsics);
            _lastAppliedTags = compaction.getLastTags();
            _intrinsics.setDeleted(_content == Resolved.UNDEFINED);
            _intrinsics.setSignature(parseHash(_compactionCutoffSignature));
            _intrinsics.setLastMutateAt(compaction.getLastContentMutation());
            _intrinsics.setLastUpdateAt(compaction.getCutoff());
        }
    }

    @Override
    public void update(UUID changeId, Delta delta, Set<String> tags) {
        _changeIds.add(changeId);

        // Evaluate the delta.
        Object updated = DeltaEvaluator.eval(delta, _content, _intrinsics);

        boolean contentChanged = !Objects.equal(_content, updated);

        // The caller may want to know if a particular delta modified the object.
        // If it didn't, the caller can suppress databus events for it. Note: a
        // delta may be made redundant by its preceding deltas but not vice versa.
        // We look at the updated content *and* the last updated tags.
        // For performance, databus dedups the redundant deltas.
        // If the delta is redundant, but has a different tag, then consider it *not* redundant
        // as it is possible that a caller listening to different tags may never be notified of a delta.
        if (!contentChanged && _lastAppliedTags.equals(tags)) {
            _redundantChangeIds.add(changeId);
        } else if (_lastMutationId == null || !changeId.equals(_compactionCutoffId)) {
            // Record this as the most recent change which mutated the content and/or tags
            _lastMutationId = changeId;
        }

        _content = updated;
        _lastAppliedTags = tags;

        // Update the intrinsics for the next time through the loop and for the result
        _intrinsics.setDeleted(_content == Resolved.UNDEFINED);
        _intrinsics.setVersion(_intrinsics.getVersion() + 1);
        if (changeId.equals(_compactionCutoffId)) {
            _intrinsics.setSignature(parseHash(_compactionCutoffSignature));
        } else {
            _intrinsics.setSignature(hash(_intrinsics.getSignatureHashCode(), changeId));
        }
        if (_intrinsics.getFirstUpdateAt() == null) {
            _intrinsics.setFirstUpdateAt(changeId);
        }
        _intrinsics.setLastUpdateAt(changeId);

        // When evaluating whether to update the "lastMutateAt" intrinsic we also have check whether the existing
        // value is null.  In the corner case where the first delta for a record is a deletion we still want
        // to set the intrinsic value to the initial delta.
        if (contentChanged || _intrinsics.getLastMutateAt() == null) {
            _intrinsics.setLastMutateAt(changeId);
        }
    }

    @Override
    public Resolved resolved() {
        return new Resolved(_content, _intrinsics, _compactionCutoffId, _lastCompactedMutationId, _lastMutationId,
                _changeIds, _redundantChangeIds, _lastAppliedTags);
    }

    private HashCode parseHash(String string) {
        try {
            return HashCode.fromBytes(Hex.decodeHex(string.toCharArray()));
        } catch (DecoderException e) {
            Throwables.propagateIfPossible(e);
            throw new RuntimeException(e);
        }
    }

    private HashCode hash(HashCode previous, UUID next) {
        Hasher hasher = HASH_FUNCTION.newHasher();
        hasher.putBytes(previous.asBytes());
        hasher.putBytes(UUIDs.asByteArray(next));
        return hasher.hash();
    }
}
