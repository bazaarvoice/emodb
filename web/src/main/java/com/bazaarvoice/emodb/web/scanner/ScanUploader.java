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
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Entry point for uploading JSON representations of a placement to a file system, such as S3.
 * The actual uploading takes place asynchronously the following classes:
 * <p/>
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

    public ScanStatus scanAndUpload(String scanId, ScanOptions options) {
        return scanAndUpload(scanId, options, false);
    }

    public ScanStatus scanAndUpload(String scanId, ScanOptions options, boolean dryRun) {
        ScanPlan plan = createPlan(scanId, options);
        ScanStatus status = plan.toScanStatus();

        if (!dryRun) {
            startScanUpload(scanId, status);
        }

        return status;
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

    private void startScanUpload(String scanId, ScanStatus status) {
        boolean scanCreated = false;

        try {
            // compaction control timestamp = stash start time + 1 minutes buffer time. This is needed to allow the setting time to trickle the request to the DataStore.
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

        try {
            // Create the scan
            _scanStatusDAO.updateScanStatus(status);
            scanCreated = true;

            // Notify the workflow that the scan can be started
            _scanWorkflow.scanStatusUpdated(scanId);

            // Send notification that the scan has started
            _stashStateListener.stashStarted(status.asPluginStashMetadata());
        } catch (Exception e) {
            _log.error("Failed to start scan and upload for scan {}", scanId, e);

            // Delete the entry of the scan start time in Zookeeper.
            try {
                _compactionControlSource.deleteStashTime(scanId, _dataCenters.getSelf().getName());
            } catch (Exception ex) {
                _log.error("Failed to delete the stash time for scan {}", scanId, ex);
            }

            if (scanCreated) {
                // The scan was not properly started; cancel the scan
                try {
                    _scanStatusDAO.setCanceled(scanId);
                } catch (Exception e2) {
                    // Don't mask the original exception but log it
                    _log.error("Failed to mark unsuccessfully started scan as canceled: [id={}]", scanId, e2);
                }
            }

            throw Throwables.propagate(e);
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
