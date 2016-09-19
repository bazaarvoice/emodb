package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DistributedCompactor extends AbstractCompactor implements Compactor {

    private final DefaultCompactor _legacyCompactor;

    public DistributedCompactor(Counter archiveDeltaSizeInMemory, boolean keepDeltaHistory, MetricRegistry metricRegistry) {
        super(archiveDeltaSizeInMemory, keepDeltaHistory, metricRegistry);
        _legacyCompactor = new DefaultCompactor(archiveDeltaSizeInMemory, keepDeltaHistory, metricRegistry);
    }

    public Expanded expand(Record record, long fullConsistencyTimestamp, long compactionConsistencyTimeStamp, MutableIntrinsics intrinsics,
                           boolean ignoreRecent, Supplier<Record> requeryFn) {
        // Bound the # of times we attempt to resolve race conditions--we never want to go into an infinite loop.
        for (int i = 0; i < 10; i++) {
            try {
                return doExpand(record, fullConsistencyTimestamp, compactionConsistencyTimeStamp, intrinsics, ignoreRecent);
            } catch (RestartException e) {
                // Raced another process w/processing compaction records and lost.  Re-query the record and try again.
                record = requeryFn.get();
            }
        }
        throw new IllegalStateException("Unable to resolve object, " +
                "repeated attempts failed due to apparent race conditions: " + record.getKey());
    }

    /**
     * This method creates pending compactions if there are deltas behind the Full Consistency Timestamp (FCT).
     * To avoid corruption in a distributed system, we defer the deletes of "compaction-owned" deltas or compactions
     * until the owning compaction is behind FCT.
     * Also, we try to avoid any further compactions, if there is already an outstanding compaction, which is not behind FCT yet.
     * This is because if we keep compacting and not delete deltas, then we can get in a situation when compactions will kept
     * getting "eaten" by newer compactions and their owned deltas will never get deleted.
     *
     * @throws RestartException
     */
    protected Expanded doExpand(Record record, long fullConsistencyTimestamp, long compactionConsistencyTimeStamp, MutableIntrinsics intrinsics, boolean ignoreRecent)
            throws RestartException {
        List<UUID> keysToDelete = Lists.newArrayList();
        DeltasArchive deltasArchive = new DeltasArchive();

        // Loop through the compaction records and find the one that is most up-to-date.  Obsolete ones may be deleted.
        Map.Entry<UUID, Compaction> compactionEntry = findEffectiveCompaction(record.passOneIterator(), keysToDelete, compactionConsistencyTimeStamp);

        // Check to see if this is a legacy compaction
        if (compactionEntry != null && compactionEntry.getValue().getCompactedDelta() == null) {
            // Legacy compaction found. Can't use this compactor.
            return _legacyCompactor.doExpand(record, fullConsistencyTimestamp, intrinsics, ignoreRecent, compactionEntry);
        }

        // Save the number of collected compaction Ids to delete in the pending compaction
        List<UUID> compactionKeysToDelete = Lists.newArrayList(keysToDelete.iterator());

        PeekingIterator<Map.Entry<UUID, DeltaTagPair>> deltaIterator = Iterators.peekingIterator(
                deltaIterator(record.passTwoIterator(), compactionEntry));

        UUID compactionKey = null;
        Compaction compaction = null;
        UUID cutoffId = null;
        UUID initialCutoff = null;
        Delta cutoffDelta= null;
        Delta initialCutoffDelta = null;
        boolean compactionChanged = false;

        // Stats for slow query logging ignore compaction performed as result of this method
        int numPersistentDeltas = 0;
        long numDeletedDeltas = 0;

        // Check if we should delete deltas for the selected compaction
        boolean deleteDeltasForCompaction;
        // Check if we should be creating a compaction.
        // If a compaction is found outside of the FCT, then we should hold off making new compactions.
        boolean createNewCompaction = true;

        // Initialize the resolver based on the current effective compaction state.
        Resolver resolver;
        if (compactionEntry == null) {
            resolver = new DefaultResolver(intrinsics);
        } else {
            deleteDeltasForCompaction = TimeUUIDs.getTimeMillis(compactionEntry.getKey()) < compactionConsistencyTimeStamp;
            createNewCompaction = deleteDeltasForCompaction;

            compactionKey = compactionEntry.getKey();
            compaction = compactionEntry.getValue();
            numDeletedDeltas = compaction.getCount();

            resolver = new DefaultResolver(intrinsics, compaction);

            // Concurrent compaction operations can race and leave deltas older than the chosen cutoff.  Delete those deltas.
            // Also, safe-delete: We need to make sure that compaction is fully consistent before we delete any deltas
            cutoffId = compaction.getCutoff();
            initialCutoff = compaction.getCutoff();
            while (deltaIterator.hasNext() && TimeUUIDs.compare(deltaIterator.peek().getKey(), cutoffId) <= 0) {
                if (!deleteDeltasForCompaction) {
                    deltaIterator.next();
                    continue;
                }
                Map.Entry<UUID, DeltaTagPair> deltaEntry = deltaIterator.next();
                keysToDelete.add(deltaEntry.getKey());
                numPersistentDeltas++;
            }

            // Assert that the resolved compacted content is always a literal
            assert(compaction.getCompactedDelta().isConstant()) : "Compacted delta was not a literal";

            cutoffDelta = compaction.getCompactedDelta();
            initialCutoffDelta = compaction.getCompactedDelta();
        }

        List<UUID> compactibleChangeIds = Lists.newArrayList();
        while (createNewCompaction && deltaIterator.hasNext() && TimeUUIDs.getTimeMillis(deltaIterator.peek().getKey()) < fullConsistencyTimestamp) {
            Map.Entry<UUID, DeltaTagPair> entry = deltaIterator.next();
            resolver.update(entry.getKey(), entry.getValue().delta, entry.getValue().tags);
            compactibleChangeIds.add(entry.getKey());
            // We would like to keep these deltas in pending compaction object in memory so we don't have to
            // go to C* to get them.
            deltasArchive.addDeltaArchive(entry.getKey(), entry.getValue().delta);
            numPersistentDeltas++;
        }
        if (!compactibleChangeIds.isEmpty()) {
            // Merge the N oldest deltas and write the result into the new compaction.
            Resolved resolved = resolver.resolved();

            // Note: We should *not* delete the compaction that the new compaction is based upon, and defer
            // its deletes upon a later compaction. The deletes of all compactions are taken care of simultaneously
            // while looking for most effective compaction in {@link AbstractCompactor#findEffectiveCompaction}.

            // Write a new compaction record and re-write the preceding delta with the content resolved to this point.
            compactionKey = TimeUUIDs.newUUID();
            compaction = new Compaction(
                    resolved.getIntrinsics().getVersion(),
                    resolved.getIntrinsics().getFirstUpdateAtUuid(),
                    resolved.getIntrinsics().getLastUpdateAtUuid(),
                    resolved.getIntrinsics().getSignature(),
                    resolved.getIntrinsics().getLastMutateAtUuid(),
                    resolved.getLastMutation(),
                    // Compacted Delta
                    resolved.getConstant(), resolved.getLastTags());

            cutoffId = compaction.getCutoff();
            cutoffDelta = resolved.getConstant();
            compactionChanged = true;

            // Re-initialize the resolver with the new compaction state.
            resolver = new DefaultResolver(intrinsics, compaction);
        }
        // Persist the compaction, and keys-to-delete.  We must write compaction and delete synchronously to
        // be sure that eventually consistent readers don't see the result of the deletions w/o also
        // seeing the accompanying compaction.
        PendingCompaction pendingCompaction = (compactionChanged || !keysToDelete.isEmpty()) ?
                new PendingCompaction(compactionKey, compaction, cutoffId, initialCutoff, cutoffDelta, initialCutoffDelta, keysToDelete,
                        compactionKeysToDelete, deltasArchive.deltasToArchive)
                : null;

        try {
            updateSizeCounter(pendingCompaction);
        } catch (DeltaHistorySizeExceededException ex) {
            // Too big to write via thrift - clear the delta archives
            assert pendingCompaction != null : "Unexpected NPE for pendingCompaction";
            pendingCompaction.getDeltasToArchive().clear();
        }

        // Resolve recent deltas.
        while (deltaIterator.hasNext()) {
            Map.Entry<UUID, DeltaTagPair> entry = deltaIterator.next();
            if (ignoreRecent && TimeUUIDs.getTimeMillis(entry.getKey()) >= fullConsistencyTimestamp) {
                break;
            }
            resolver.update(entry.getKey(), entry.getValue().delta, entry.getValue().tags);
            numPersistentDeltas++;
        }

        return new Expanded(resolver.resolved(), pendingCompaction, numPersistentDeltas, numDeletedDeltas);
    }
}
