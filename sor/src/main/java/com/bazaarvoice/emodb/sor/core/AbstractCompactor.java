package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.db.test.DeltaClusteringKey;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class AbstractCompactor {

    @VisibleForTesting
    protected static final Long MAX_DELTA_ARCHIVE_SIZE = 2L * 1024 * 1024; // 2 MB
    // Conservatively keep this under thrift_framed_transport_size_in_mb value from cassandra.yaml
    @VisibleForTesting
    protected static final Long MAX_TRANSPORT_SIZE = 10L * 1024 * 1024; // 10 MB

    private final Meter _discardedDeltas;
    private final Histogram _deltaSizeHistogram;
    private final Meter _discardedDeltaHistory;
    private final Counter _archiveDeltaSizeInMemory;
    private final boolean _keepDeltaHistory;


    public AbstractCompactor(Counter archiveDeltaSizeInMemory, boolean keepDeltaHistory, MetricRegistry metricRegistry) {
        _archiveDeltaSizeInMemory = checkNotNull(archiveDeltaSizeInMemory, "archiveDeltaSizeInMemory");
        _keepDeltaHistory = keepDeltaHistory;

        _discardedDeltas = metricRegistry.meter(MetricRegistry.name("bv.emodb.sor", "DefaultCompactor", "discarded_deltas"));
        _deltaSizeHistogram = metricRegistry.histogram(MetricRegistry.name("bv.emodb.sor", "DefaultCompactor", "delta_size"));
        _discardedDeltaHistory = metricRegistry.meter(MetricRegistry.name("bv.emodb.sor", "DefaultCompactor", "discarded_delta_history"));
    }

    protected Map.Entry<DeltaClusteringKey, Compaction> findEffectiveCompaction(Iterator<Map.Entry<DeltaClusteringKey, Compaction>> compactionIter,
                                                                Collection<DeltaClusteringKey> otherCompactionIds, long compactionConsistencyTimeStamp) {
        Compaction best = null;
        DeltaClusteringKey bestClusteringKey = null;
        Map.Entry<DeltaClusteringKey, Compaction> bestEntry = null;
        while (compactionIter.hasNext()) {
            Map.Entry<DeltaClusteringKey, Compaction> entry = compactionIter.next();
            DeltaClusteringKey clusteringKey = entry.getKey();
            Compaction compaction = entry.getValue();
            // The most recent compaction that covers the most "old deltas" wins
            if (compaction.getCount() > 0 && (best == null || isLessThan(bestClusteringKey.getChangeId(), best, clusteringKey.getChangeId(), compaction))) {
                if (best != null) {
                    otherCompactionIds.add(bestClusteringKey);
                }
                best = compaction;
                bestClusteringKey = clusteringKey;
                bestEntry = entry;
            } else {
                otherCompactionIds.add(clusteringKey);
            }
        }

        // Check if bestEntry is behind FCT. If so, we can get rid of others.
        if (bestEntry != null && TimeUUIDs.getTimeMillis(bestEntry.getKey().getChangeId()) >= compactionConsistencyTimeStamp) {
            // Since the bestEntry is ahead of FCT, we keep all the other compactions and defer their deletion
            otherCompactionIds.clear();
        }

        return bestEntry;
    }

    private boolean isLessThan(UUID leftId, Compaction left, UUID rightId, Compaction right) {
        return (left.getCount() < right.getCount()) ||
                (left.getCount() == right.getCount() && TimeUUIDs.compare(leftId, rightId) < 0);
    }

    public Iterator<DataAudit> getAuditedContent(final PendingCompaction pendingCompaction,
                                                 MutableIntrinsics intrinsics) {
        long versionOfFirstCompactedDelta = RowVersionUtils.getVersionRetroactively(pendingCompaction);
        Compaction compaction = pendingCompaction.getCompaction();

        // Is this going to be the very first compaction?
        boolean isFirstCompaction = versionOfFirstCompactedDelta == 0L;
        final Resolver resolver;
        if (!isFirstCompaction) {
            // Create a synthetic compaction record that will resolve to the correct version of audited content
            Compaction compactionInfo = new Compaction(versionOfFirstCompactedDelta,
                    compaction.getFirst(),
                    compaction.getCutoff(),
                    compaction.getCutoffSignature(),
                    compaction.getLastContentMutation(),
                    compaction.getLastMutation(),
                    pendingCompaction.getStartingDelta(), compaction.getLastTags());
            resolver = new DefaultResolver(intrinsics, compactionInfo);
        } else {
            resolver = new DefaultResolver(intrinsics);
        }

        final Iterator<Map.Entry<UUID, Delta>> auditIterator = pendingCompaction.getDeltasToArchive().iterator();

        return new AbstractIterator<DataAudit>() {
            @Override
            protected DataAudit computeNext() {
                if (auditIterator.hasNext()) {
                    final Map.Entry<UUID, Delta> entry = auditIterator.next();
                    resolver.update(entry.getKey(), entry.getValue(), ImmutableSet.of());
                    return new DataAudit() {
                        @Override
                        public UUID getChangeId() {
                            return entry.getKey();
                        }

                        @Override
                        public Resolved getResolved() {
                            return resolver.resolved();
                        }

                        @Override
                        public Delta getDelta() {
                            return entry.getValue();
                        }
                    };
                }
                return endOfData();
            }
        };
    }

    protected Iterator<Map.Entry<DeltaClusteringKey, DeltaTagPair>> deltaIterator(final Iterator<Map.Entry<DeltaClusteringKey, Change>> changeIter,
                                                           final Map.Entry<DeltaClusteringKey, Compaction> effectiveCompaction) {
        return new AbstractIterator<Map.Entry<DeltaClusteringKey, DeltaTagPair>>() {
            @Override
            protected Map.Entry<DeltaClusteringKey, DeltaTagPair> computeNext() {
                while (changeIter.hasNext()) {
                    Map.Entry<DeltaClusteringKey, Change> entry = changeIter.next();
                    Change change = entry.getValue();
                    if (change.getDelta() != null) {
                        return Maps.immutableEntry(entry.getKey(), new DeltaTagPair(change.getDelta(), change.getTags()));
                    }

                    // Earlier we picked the "best" compaction record and we're using that to resolve the object.
                    // If, during this scan, we encounter an even better compaction record, then we're resolving
                    // potentially stale data.  We must restart the resolution process using the new better record.
                    if (change.getCompaction() != null) {
                        if (effectiveCompaction == null ||
                                isLessThan(
                                        effectiveCompaction.getKey().getChangeId(), effectiveCompaction.getValue(),
                                        change.getId(), change.getCompaction())) {
                            throw new RestartException();
                        }
                    }
                }
                return endOfData();
            }
        };
    }

    protected void updateSizeCounter(@Nullable PendingCompaction pendingCompaction) {
        if (pendingCompaction == null) {
            return;
        }
        long deltaSizeInBytes = 0L;
        for (Map.Entry<UUID, Delta> deltaEntry : pendingCompaction.getDeltasToArchive()) {
            long deltaSize = deltaEntry.getValue().size();
            deltaSizeInBytes += deltaSize;
            _deltaSizeHistogram.update(deltaSize);
        }

        // One last check to make sure we don't violate transport protocol size limit
        // Delta History saves history for each compacted delta that includes
        // 1) Actual Delta, and 2) Complete resolved content as of that Delta
        // While we do have the size of actual deltas, we need to account for the resolved content that will get saved
        // with each delta history. To get that, we will assume that the actual content size for each delta is going to
        // be roughly the size of the final compacted delta literal. We do not want to resolve content for each delta
        // here.
        // TotalDeltaArchiveSize = deltaSize + ContentSize;

        long eachDeltaContentSize = pendingCompaction.getCompaction().getCompactedDelta().size();
        long totalDeltaArchiveSize = deltaSizeInBytes +
                (pendingCompaction.getDeltasToArchive().size() * eachDeltaContentSize);
        if (totalDeltaArchiveSize >= MAX_TRANSPORT_SIZE) {
            _discardedDeltaHistory.mark();
            throw new DeltaHistorySizeExceededException();
        }

        _archiveDeltaSizeInMemory.inc(deltaSizeInBytes);
    }

    protected static class RestartException extends RuntimeException {
    }

    protected static class DeltaHistorySizeExceededException extends RuntimeException {
    }

    protected class DeltasArchive {
        private boolean keepHistory;
        protected List<Map.Entry<UUID, Delta>> deltasToArchive = Lists.newArrayList();

        protected DeltasArchive() {
            this.keepHistory = _keepDeltaHistory;
        }

        void addDeltaArchive(UUID changeId, Delta delta) {
            if (!keepHistory || checkIfDeltaIsTooLarge(delta)) {
                // If a huge delta is found, then make sure that there are no deltas to archive
                deltasToArchive.clear();
                _discardedDeltas.mark();
                // Since archived deltas are taking more memory than we want, we will not keep delta archives for this
                // round of compaction.
                keepHistory = false;
            } else {
                deltasToArchive.add(Maps.immutableEntry(changeId, delta));
            }
        }

        /**
         * This only checks against the completed pending compactions that are in memory currently,
         * but not against the pending compactions that are in progress
         */
        boolean checkIfDeltaIsTooLarge(Delta delta) {
            return delta.size() + _archiveDeltaSizeInMemory.getCount() >= MAX_DELTA_ARCHIVE_SIZE;
        }
    }

    protected class DeltaTagPair {
        public final Delta delta;
        public final Set<String> tags;
        protected DeltaTagPair(Delta delta, Set<String> tags) {
            this.delta = delta;
            this.tags = tags;
        }
    }
}
