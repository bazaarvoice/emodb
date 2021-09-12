package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.db.test.DeltaClusteringKey;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DefaultCompactor extends AbstractCompactor {

    private final Logger _log = LoggerFactory.getLogger(DefaultCompactor.class);

    public DefaultCompactor(Counter archiveDeltaSizeInMemory, boolean keepDeltaHistory, MetricRegistry metricRegistry) {
        super(archiveDeltaSizeInMemory, keepDeltaHistory, metricRegistry);
    }

    public Expanded doExpand(Record record, long fullConsistencyTimestamp, MutableIntrinsics intrinsics, boolean ignoreRecent,
                                Map.Entry<DeltaClusteringKey, Compaction> compactionEntry)
            throws RestartException {
        checkNotNull(compactionEntry, "A legacy compaction needs to be provided to this legacy compactor");
        List<DeltaClusteringKey> keysToDelete = Lists.newArrayList();
        DeltasArchive deltasArchive = new DeltasArchive();

        // Save the number of collected compaction Ids to delete in the pending compaction
        List<DeltaClusteringKey> compactionKeysToDelete = Lists.newArrayList(keysToDelete.iterator());

        PeekingIterator<Map.Entry<DeltaClusteringKey, DeltaTagPair>> deltaIterator = Iterators.peekingIterator(
                deltaIterator(record.passTwoIterator(), compactionEntry));

        DeltaClusteringKey compactionKey = compactionEntry.getKey();
        Compaction compaction = compactionEntry.getValue();
        UUID cutoffId = compaction.getCutoff();
        UUID initialCutoff = compaction.getCutoff();
        boolean compactionChanged = false;

        // Stats for slow query logging ignore compaction performed as result of this method
        int numPersistentDeltas = 0;
        long numDeletedDeltas = compaction.getCount();

        // Initialize the resolver based on the current effective compaction state.
        Resolver resolver;

        resolver = new DefaultResolver(intrinsics, compaction);

        // Concurrent compaction operations can race and leave deltas older than the chosen cutoff.  Delete those deltas.
        while (deltaIterator.hasNext() && TimeUUIDs.compare(deltaIterator.peek().getKey().getChangeId(), cutoffId) < 0) {
            keysToDelete.add(deltaIterator.next().getKey());
            numPersistentDeltas++;
        }
        // The next delta in the sequence must exist and be cutoffId.  If it's not, we have lost data somewhere?!
        if (!deltaIterator.peek().getKey().getChangeId().equals(cutoffId)) {
            // The only legitimate reason for the cutoff delta to be missing is because it was compacted away.
            // Scan for the compaction that replaced it to trigger a RestartException, assert if we don't find it.
            Iterators.advance(deltaIterator, Integer.MAX_VALUE);
            checkState(false, record.getKey());
        }
        if (!deltaIterator.peek().getValue().delta.isConstant()) {
            _log.debug("Compaction cutoff was not a literal; forcing restart");
            throw new RestartException();
        }

        Delta cutoffDelta = deltaIterator.peek().getValue().delta;
        Delta initialCutoffDelta = deltaIterator.peek().getValue().delta;


        // Resolve deltas older than the current fullConsistencyTimestamp.  These deltas may be compacted together.
        List<DeltaClusteringKey> compactibleClusteringKeys = Lists.newArrayList();
        while (deltaIterator.hasNext() && TimeUUIDs.getTimeMillis(deltaIterator.peek().getKey().getChangeId()) < fullConsistencyTimestamp) {
            Map.Entry<DeltaClusteringKey, DeltaTagPair> entry = deltaIterator.next();
            resolver.update(entry.getKey().getChangeId(), entry.getValue().delta, entry.getValue().tags);
            compactibleClusteringKeys.add(entry.getKey());
            // We would like to keep these deltas in pending compaction object in memory so we don't have to
            // go to C* to get them.
            deltasArchive.addDeltaArchive(entry.getKey().getChangeId(), entry.getValue().delta);
            numPersistentDeltas++;
        }
        if (!compactibleClusteringKeys.isEmpty()) {
            // Merge the N oldest deltas and write the result into the new compaction.
            Resolved resolved = resolver.resolved();

            // Delete old compaction record and old deltas.
            keysToDelete.add(compactionKey);
            compactionKeysToDelete.add(compactionKey);
            keysToDelete.addAll(compactibleClusteringKeys);

            // Write a new compaction record and re-write the preceding delta with the content resolved to this point.
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
                new PendingCompaction(TimeUUIDs.newUUID(), compaction, cutoffId, initialCutoff, cutoffDelta, initialCutoffDelta, keysToDelete,
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
        if (!ignoreRecent) {
            while (deltaIterator.hasNext()) {
                Map.Entry<DeltaClusteringKey, DeltaTagPair> entryToTransform = deltaIterator.next();
                Map.Entry<UUID, DeltaTagPair> entry = Maps.immutableEntry(entryToTransform.getKey().getChangeId(), entryToTransform.getValue());
                resolver.update(entry.getKey(), entry.getValue().delta, entry.getValue().tags);
                numPersistentDeltas++;
            }
        }

        return new Expanded(resolver.resolved(), pendingCompaction, numPersistentDeltas, numDeletedDeltas);
    }
}
