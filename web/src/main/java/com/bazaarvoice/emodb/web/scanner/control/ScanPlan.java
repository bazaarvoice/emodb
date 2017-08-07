package com.bazaarvoice.emodb.web.scanner.control;

import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanRangeStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to create and manage a plan for performing a scan and upload operation on one or more placements.
 *
 * The plan is built such that batches of work are organized based on the underlying resources that will be used
 * when the scan is performed.  These resources are abstracted away as clusters.  For example, assume a scan were
 * to take place over the placements "ugc_global:ugc", "ugc_us:ugc", and "catalog_global:cat".  Since the two UGC
 * placements reside on the same Cassandra ring the clusters for both will be the same.  Consequently the plan
 * will restrict the number of concurrent operations on the two UGC placements while allowing complete parallelization
 * with the catalog placement.
 *
 * Each batch contains one or more items.  Each item contains a collection of scan ranges which are all owned by the
 * same Cassandra host and therefore contend for the same resources.
 *
 * In summary:
 *
 * <ul>
 *     <li>
 *         The plan consists of batches.  Operating on each batch serially ensures there are no hot-spots on the ring.
 *     </li>
 *     <li>
 *         Each batch consists of plan items.  All plan items in a batch can be scanned in parallel without creating
 *         hot-spots on the ring.
 *     </li>
 *     <li>
 *         Each plan item consists of scan ranges.  The resource utilization for a token range in the ring increases
 *         proportionally to the number of parallel scans performed on a plan item.
 *     </li>
 * </ul>
 */
public class ScanPlan {

    private final String _scanId;
    private final ScanOptions _options;
    private final Map<String, PlanBatch> _clusterHeads = Maps.newHashMap();
    private final Map<String, PlanBatch> _clusterTails = Maps.newHashMap();

    public ScanPlan(String scanId, ScanOptions options) {
        _scanId = scanId;
        _options = options;
    }

    /**
     * Starts a new batch for a cluster.  Future calls to {@link #addTokenRangeToCurrentBatchForCluster(String, String, java.util.Collection)}
     * are added to the batch started by this call for the corresponding cluster.
     */
    public void startNewBatchForCluster(String cluster) {
        // Only start a new batch if the current one is non-empty
        PlanBatch batch = _clusterTails.get(cluster);
        if (batch != null && !batch.getItems().isEmpty()) {
            PlanBatch newBatch = new PlanBatch();
            batch.setNextBatch(newBatch);
            _clusterTails.put(cluster, newBatch);
        }
    }

    /**
     * Adds a collection of scan ranges to the plan for a specific placement.  The range collection should all belong
     * to a single token range in the ring.
     * @param cluster An identifier for the underlying resources used to scan the placement (typically an identifier for the Cassandra ring)
     * @param placement The name of the placement
     * @param ranges All non-overlapping sub-ranges from a single token range in the ring
     */
    public void addTokenRangeToCurrentBatchForCluster(String cluster, String placement, Collection<ScanRange> ranges) {
        PlanBatch batch = _clusterTails.get(cluster);
        if (batch == null) {
            batch = new PlanBatch();
            _clusterHeads.put(cluster, batch);
            _clusterTails.put(cluster, batch);
        }
        batch.addPlanItem(new PlanItem(placement, ranges));
    }

    /**
     * Creates a ScanStatus based on the current state of the plan.  All scan ranges are added as pending tasks.
     */
    public ScanStatus toScanStatus() {
        List<ScanRangeStatus> pendingRangeStatuses = Lists.newArrayList();

        // Unique identifier for each scan task
        int taskId = 0;
        // Unique identifier for each batch
        int batchId = 0;
        // Unique identifier which identifies all tasks which affect the same resources in the ring
        int concurrencyId = 0;

        for (PlanBatch batch : _clusterHeads.values()) {
            // Within a single Cassandra ring each batch runs serially.  This is enforced by blocking on the prior batch
            // from the same cluster.
            Integer lastBatchId = null;

            while (batch != null) {
                List<PlanItem> items = batch.getItems();
                if (!items.isEmpty()) {
                    Optional<Integer> blockingBatch = Optional.fromNullable(lastBatchId);
                    for (PlanItem item : items) {
                        String placement = item.getPlacement();
                        // If the token range is subdivided into more than one scan range then mark all sub-ranges with
                        // the same unique concurrency ID.
                        Optional<Integer> concurrency = item.getScanRanges().size() > 1 ? Optional.of(concurrencyId++) : Optional.<Integer>absent();

                        for (ScanRange scanRange : item.getScanRanges()) {
                            pendingRangeStatuses.add(new ScanRangeStatus(
                                    taskId++, placement, scanRange, batchId, blockingBatch, concurrency));
                        }
                    }
                    lastBatchId = batchId;
                    batchId++;
                }
                batch = batch.getNextBatch();
            }
        }

        return new ScanStatus(_scanId, _options, false, false, new Date(), pendingRangeStatuses,
                ImmutableList.<ScanRangeStatus>of(), ImmutableList.<ScanRangeStatus>of());
    }

    public static class PlanItem {
        private final String _placement;
        private final Set<ScanRange> _scanRanges;

        public PlanItem(String placement, Collection<ScanRange> scanRanges) {
            _placement = placement;
            _scanRanges = ImmutableSortedSet.copyOf(scanRanges);
        }

        public String getPlacement() {
            return _placement;
        }

        public Set<ScanRange> getScanRanges() {
            return _scanRanges;
        }
    }

    public static class PlanBatch {
        private final List<PlanItem> _items = Lists.newArrayList();
        private PlanBatch _nextBatch;

        public List<PlanItem> getItems() {
            return _items;
        }

        void addPlanItem(PlanItem item) {
            _items.add(item);
        }

        void setNextBatch(PlanBatch nextBatch) {
            _nextBatch = nextBatch;
        }

        PlanBatch getNextBatch() {
            return _nextBatch;
        }
    }
}
