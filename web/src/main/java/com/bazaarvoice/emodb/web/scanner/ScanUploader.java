package com.bazaarvoice.emodb.web.scanner;

import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.compactioncontrol.DelegateCompactionControl;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.db.ScanRangeSplits;
import com.bazaarvoice.emodb.web.scanner.control.ScanPlan;
import com.bazaarvoice.emodb.web.scanner.control.ScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanRangeStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatusDAO;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Entry point for uploading JSON representations of a placement to a file system, such as S3.
 * The actual uploading takes place asynchronously the following classes:
 * <p>
 * <ul>
 * <li>
 * {@link com.bazaarvoice.emodb.web.scanner.control.ScanUploadMonitor}: monitors all active uploads and schedules
 * new token ranges for uploading as they become available.</li>
 * </li>
 * <li>
 * {@link com.bazaarvoice.emodb.web.scanner.control.DistributedScanRangeMonitor}: listens for token ranges
 * available for scanning and scans and uploads them locally.
 * </li>
 * </ul>
 */
public class ScanUploader {

    private static final Logger _log = LoggerFactory.getLogger(ScanUploader.class);

    private long _compactionControlBufferTimeInMillis = Duration.ofMinutes(1).toMillis();
    private long _scanWaitTimeInMillis = Duration.ofMinutes(5).toMillis();

    private final DataTools _dataTools;
    private final ScanWorkflow _scanWorkflow;
    private final ScanStatusDAO _scanStatusDAO;
    private final StashStateListener _stashStateListener;
    private final CompactionControlSource _compactionControlSource;
    private final DataCenters _dataCenters;

    @Inject
    public ScanUploader(DataTools dataTools, ScanWorkflow scanWorkflow, ScanStatusDAO scanStatusDAO,
                        StashStateListener stashStateListener, @DelegateCompactionControl CompactionControlSource compactionControlSource, DataCenters dataCenters) {
        _dataTools = checkNotNull(dataTools, "dataTools");
        _scanWorkflow = checkNotNull(scanWorkflow, "scanWorkflow");
        _scanStatusDAO = checkNotNull(scanStatusDAO, "scanStatusDAO");
        _stashStateListener = checkNotNull(stashStateListener, "stashStateListener");
        _compactionControlSource = checkNotNull(compactionControlSource, "compactionControlSource");
        _dataCenters = checkNotNull(dataCenters, "dataCenters");
    }

    @VisibleForTesting
    public void setCompactionControlBufferTimeInMillis(long compactionControlBufferTimeInMillis) {
        _compactionControlBufferTimeInMillis = compactionControlBufferTimeInMillis;
    }

    @VisibleForTesting
    public void setScanWaitTimeInMillis(long scanWaitTimeInMillis) {
        _scanWaitTimeInMillis = scanWaitTimeInMillis;
    }

    public ScanAndUploadBuilder scanAndUpload(String scanId, ScanOptions options) {
        return new ScanAndUploadBuilder(scanId, options);
    }

    public class ScanAndUploadBuilder {
        private final String _scanId;
        private final ScanOptions _options;
        private boolean _dryRun = false;
        private String _usePlanFrom;

        private ScanAndUploadBuilder(String scanId, ScanOptions options) {
            _scanId = scanId;
            _options = options;
        }

        public ScanAndUploadBuilder dryRun(boolean dryRun) {
            _dryRun = dryRun;
            return this;
        }

        public ScanAndUploadBuilder usePlanFromStashId(String existingId) {
            _usePlanFrom = existingId;
            return this;
        }

        public ScanStatus start() {
            ScanStatus status;
            if (_usePlanFrom == null) {
                ScanPlan plan = createPlan(_scanId, _options);
                status = plan.toScanStatus();
            } else {
                ScanStatus existingStatus = _scanStatusDAO.getScanStatus(_usePlanFrom);
                if (existingStatus == null) {
                    throw new IllegalStateException("Cannot repeat from unknown scan: " + _usePlanFrom);
                }
                status = createNewScanFromExistingScan(_scanId, _options, existingStatus);
            }
            status.setCompactionControlBufferTimeInMillis(_compactionControlBufferTimeInMillis);

            if (!_dryRun) {
                startScanUpload(_scanId, status, _options.getPlacements());
            }

            return status;
        }
    }

    /**
     * Returns a ScanPlan based on the Cassandra rings and token ranges.
     */
    private ScanPlan createPlan(String scanId, ScanOptions options) {
        ScanPlan plan = new ScanPlan(scanId, options);

        for (String placement : options.getPlacements()) {
            String cluster = _dataTools.getPlacementCluster(placement);
            ScanRangeSplits scanRangeSplits = _dataTools.getScanRangeSplits(placement, options.getRangeScanSplitSize(), Optional.<ScanRange>absent());

            if (!options.isScanByAZ()) {
                // Optionally we can reduce load across the ring by limiting scans AZ at a time.  However, the caller
                // has requested to scan all token ranges as quickly as possible, so collapse all token ranges into a
                // single group.
                scanRangeSplits = scanRangeSplits.combineGroups();
            }

            for (ScanRangeSplits.SplitGroup splitGroup : scanRangeSplits.getSplitGroups()) {
                // Start a new batch, indicating the subsequent token ranges can be scanned in parallel
                plan.startNewBatchForCluster(cluster);
                // Add the scan ranges associated with each token range in the split group to the batch
                for (ScanRangeSplits.TokenRange tokenRange : splitGroup.getTokenRanges()) {
                    plan.addTokenRangeToCurrentBatchForCluster(cluster, placement, tokenRange.getScanRanges());
                }
            }
        }

        return plan;
    }

    /**
     * Takes an existing ScanStatus and creates a new plan from it.  This method validates that a plan existed for each
     * placement in the existing ScanStatus but otherwise recreates the plan exactly as it ran.  This means that any
     * ScanOptions related to generating the plan, such as {@link ScanOptions#getRangeScanSplitSize()} and
     * {@link ScanOptions#isScanByAZ()}, will have no effect.
     */
    private ScanStatus createNewScanFromExistingScan(String scanId, ScanOptions scanOptions, ScanStatus existingScanStatus) {
        List<ScanRangeStatus> pendingScanRangeStatuses = Lists.newArrayList();
        Multimap<String, ScanRangeStatus> scanRangeStatusesByPlacement = HashMultimap.create();

        for (ScanRangeStatus scanRangeStatus : existingScanStatus.getAllScanRanges()) {
            scanRangeStatusesByPlacement.put(scanRangeStatus.getPlacement(),
                    new ScanRangeStatus(
                            scanRangeStatus.getTaskId(), scanRangeStatus.getPlacement(), scanRangeStatus.getScanRange(),
                            scanRangeStatus.getBatchId(), scanRangeStatus.getBlockedByBatchId(), scanRangeStatus.getConcurrencyId()));
        }

        for (String placement : scanOptions.getPlacements()) {
            Collection<ScanRangeStatus> scanRangeStatusesForPlacement = scanRangeStatusesByPlacement.get(placement);

            if (scanRangeStatusesForPlacement.isEmpty()) {
                throw new IllegalStateException(String.format("Previous scan \"%s\" had no plan for placement \"%s\"", scanId, placement));
            }

            pendingScanRangeStatuses.addAll(scanRangeStatusesForPlacement);
        }

        return new ScanStatus(scanId, scanOptions, false, false, new Date(), pendingScanRangeStatuses,
                ImmutableList.of(), ImmutableList.of());
    }

    private void startScanUpload(String scanId, ScanStatus status, Set<String> placements) {
        try {
            // compaction control timestamp = stash start time + 1 minute buffer time. This is needed to allow the setting time to trickle the request to the DataStore.
            // Setting the time in the future takes care of the issue of there being any in-flight compactions
            // Note: the same compaction control timestamp with 1 minute buffer time is also considered during the multiscan deltas/compactions resolving.
            long compactionControlTime = status.getCompactionControlTime().getTime();
            // expired time for now is designed to be 10 hours from the compaction control time.
            long expireTime = compactionControlTime + Duration.ofHours(10).toMillis();
            // Update the scan start time in Zookeeper in all data centers.
            _compactionControlSource.updateStashTime(scanId, compactionControlTime, Lists.newArrayList(status.getOptions().getPlacements()), expireTime, _dataCenters.getSelf().getName());
        } catch (Exception e) {
            _log.error("Failed to update the stash time for scan {}", scanId, e);
            throw Throwables.propagate(e);
        }


        // Create the scan
        try {
            _scanStatusDAO.updateScanStatus(status);
        } catch (Exception e) {
            doExceptionTasks(scanId, e);
        }

        // spawn a thread and do the below asynchronously as we would have to wait for a minute to continue to scan and we don't to include that delay in here for responding.
        new Thread(() ->
        {
            try {
                // We would like to wait for 5 minutes here to continue to scan to make sure scan don't miss any deltas that are written before the compaction control time.
                Thread.sleep(_scanWaitTimeInMillis);

                // Notify the workflow that the scan can be started
                _scanWorkflow.scanStatusUpdated(scanId);

                // Send notification that the scan has started
                _stashStateListener.stashStarted(status.asPluginStashMetadata());
            } catch (Exception e) {
                doExceptionTasks(scanId, e);
            }
        }).start();
    }

    /**
     * This is the helper routine to do few tasks in case of exception of starting a scan.
     */
    public void doExceptionTasks(String scanId, Exception e) {
        _log.error("Failed to start scan and upload for scan {}", scanId, e);

        // Delete the entry of the scan start time in Zookeeper.
        try {
            _compactionControlSource.deleteStashTime(scanId, _dataCenters.getSelf().getName());
        } catch (Exception ex) {
            _log.error("Failed to delete the stash time for scan {}", scanId, ex);
        }

        // The scan was not properly started; cancel the scan
        try {
            _scanStatusDAO.setCanceled(scanId);
        } catch (Exception e2) {
            _log.error("Failed to set the status to cancelled. ScanId: {}", scanId, e2);
        }
    }

    /**
     * Sometimes due to unexpected errors while submitting scan ranges to the underlying queues a scan can get stuck.
     * This method takes all available tasks for a scan and resubmits them.  This method is safe because
     * the underlying system is resilient to task resubmissions and concurrent work on the same task.
     */
    public ScanStatus resubmitWorkflowTasks(String scanId) {
        ScanStatus status = _scanStatusDAO.getScanStatus(scanId);
        if (status == null) {
            return null;
        }

        if (status.getCompleteTime() == null) {
            // Resubmit any active tasks
            for (ScanRangeStatus active : status.getActiveScanRanges()) {
                _scanWorkflow.addScanRangeTask(scanId, active.getTaskId(), active.getPlacement(), active.getScanRange());
            }

            // Send notification to evaluate whether any new range tasks can be started
            _scanWorkflow.scanStatusUpdated(scanId);
        }

        return status;
    }

    public ScanStatus getStatus(String id) {
        return _scanStatusDAO.getScanStatus(id);
    }

    public void cancel(String id) {
        _scanStatusDAO.setCanceled(id);

        // Notify the workflow the scan status was updated
        _scanWorkflow.scanStatusUpdated(id);

        try {
            // Delete the entry of the scan start time in Zookeeper.
            _compactionControlSource.deleteStashTime(id, _dataCenters.getSelf().getName());
        } catch (Exception e) {
            _log.error("Failed to delete the stash time for scan {}", id, e);
        }
    }
}
