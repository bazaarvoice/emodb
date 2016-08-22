package com.bazaarvoice.emodb.web.scanner;

import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
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
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Entry point for uploading JSON representations of a placement to a file system, such as S3.
 * The actual uploading takes place asynchronously the following classes:
 *
 * <ul>
 *     <li>
 *         {@link com.bazaarvoice.emodb.web.scanner.control.ScanUploadMonitor}: monitors all active uploads and schedules
 *         new token ranges for uploading as they become available.</li>
 *     </li>
 *     <li>
 *         {@link com.bazaarvoice.emodb.web.scanner.control.DistributedScanRangeMonitor}: listens for token ranges
 *         available for scanning and scans and uploads them locally.
 *     </li>
 * </ul>
 */
public class ScanUploader {

    private static final Logger _log = LoggerFactory.getLogger(ScanUploader.class);

    private final DataTools _dataTools;
    private final ScanWorkflow _scanWorkflow;
    private final ScanStatusDAO _scanStatusDAO;
    private final StashStateListener _stashStateListener;

    @Inject
    public ScanUploader(DataTools dataTools, ScanWorkflow scanWorkflow, ScanStatusDAO scanStatusDAO,
                        StashStateListener stashStateListener) {
        _dataTools = checkNotNull(dataTools, "dataTools");
        _scanWorkflow = checkNotNull(scanWorkflow, "scanWorkflow");
        _scanStatusDAO = checkNotNull(scanStatusDAO, "scanStatusDAO");
        _stashStateListener = checkNotNull(stashStateListener, "stashStateListener");
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
            // Create the scan
            _scanStatusDAO.updateScanStatus(status);
            scanCreated = true;

            // Notify the workflow that the scan can be started
            _scanWorkflow.scanStatusUpdated(scanId);

            // Send notification that the scan has started
            _stashStateListener.stashStarted(status.asPluginStashMetadata());
        } catch (Exception e) {
            _log.error("Failed to start scan and upload for scan {}", scanId, e);

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

    }
}
