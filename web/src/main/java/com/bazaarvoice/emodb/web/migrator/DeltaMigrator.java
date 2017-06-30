package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.db.ScanRangeSplits;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.control.ScanPlan;
import com.bazaarvoice.emodb.web.scanner.control.ScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatusDAO;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeltaMigrator {

    private static final Logger _log = LoggerFactory.getLogger(DeltaMigrator.class);

    private final DataTools _dataTools;
    private final ScanStatusDAO _statusDAO;
    private final ScanWorkflow _workflow;
    private final StashStateListener _listener;

    @Inject
    public DeltaMigrator(DataTools dataTools, ScanStatusDAO statusDAO, ScanWorkflow workflow,
                         StashStateListener listener) {
        _dataTools = dataTools;
        _statusDAO = statusDAO;
        _workflow = workflow;
        _listener = listener;
    }

    public ScanStatus migratePlacement(String placement) {
        ScanOptions options = new ScanOptions(placement)
                .setScanByAZ(true);
        ScanPlan plan = createPlan(placement, options);
        ScanStatus status = plan.toScanStatus();

        startMigration(placement, status);

        return status;

    }

    private ScanPlan createPlan(String id, ScanOptions options) {
        ScanPlan plan = new ScanPlan(id, options);

        for (String placement : options.getPlacements()) {
            String cluster = _dataTools.getPlacementCluster(placement);
            ScanRangeSplits scanRangeSplits = _dataTools.getScanRangeSplits(placement, options.getRangeScanSplitSize(), Optional.<ScanRange>absent());

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

    private void startMigration(String id, ScanStatus status) {
        boolean scanCreated = false;

        try {
            // Create the scan
            _statusDAO.updateScanStatus(status);
            scanCreated = true;

            // Notify the workflow that the scan can be started
            _workflow.scanStatusUpdated(id);

            // Send notification that the scan has started
            _listener.stashStarted(status.asPluginStashMetadata());
        } catch (Exception e) {
            _log.error("Failed to start migration for {}", id, e);

            if (scanCreated) {
                // The scan was not properly started; cancel the scan
                try {
                    _statusDAO.setCanceled(id);
                } catch (Exception e2) {
                    // Don't mask the original exception but log it
                    _log.error("Failed to mark unsuccessfully started scan as canceled: [id={}]", id, e2);
                }
            }

            throw Throwables.propagate(e);
        }
    }

    public ScanStatus getStatus(String id) {
        return _statusDAO.getScanStatus(id);
    }
}
