package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.db.ScanRangeSplits;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
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

public class DeltaMigrator {

    private static final Logger _log = LoggerFactory.getLogger(DeltaMigrator.class);

    private final DataTools _dataTools;
    private final ScanStatusDAO _statusDAO;
    private final ScanWorkflow _workflow;
    private final StashStateListener _listener;

    @Inject
    public DeltaMigrator(DataTools dataTools, ScanStatusDAO statusDAO, ScanWorkflow workflow,
                         StashStateListener listener) {
        _dataTools = checkNotNull(dataTools, "dataTools");
        _statusDAO = checkNotNull(statusDAO, "statusDAO");
        _workflow = checkNotNull(workflow, "workflow");
        _listener = checkNotNull(listener, "listener");
    }

    public ScanStatus migratePlacement(String placement, String migrationId) {
        ScanOptions options = new ScanOptions(placement)
                .setScanByAZ(true);
        ScanPlan plan = createPlan(migrationId, options);
        ScanStatus status = plan.toScanStatus();

        startMigration(migrationId, status);

        return status;

    }

    private ScanPlan createPlan(String id, ScanOptions options) {
        ScanPlan plan = new ScanPlan(id, options);

        for (String placement : options.getPlacements()) {
            String cluster = _dataTools.getPlacementCluster(placement);
            ScanRangeSplits scanRangeSplits = _dataTools.getScanRangeSplits(placement, options.getRangeScanSplitSize(), Optional.<ScanRange>absent());

            for (ScanRangeSplits.SplitGroup splitGroup : scanRangeSplits.getSplitGroups()) {
                // Start a new batch, indicating the subsequent token ranges can be migrated in parallel
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
        boolean migrationCreated = false;

        try {
            // Create the migration
            _statusDAO.updateScanStatus(status);
            migrationCreated = true;

            // Notify the workflow that the migration can be started
            _workflow.scanStatusUpdated(id);

            // Send notification that the migration has started
            _listener.stashStarted(status.asPluginStashMetadata());
        } catch (Exception e) {
            _log.error("Failed to start migration for {}", id, e);

            if (migrationCreated) {
                // The migrator was not properly started; cancel the migration
                try {
                    _statusDAO.setCanceled(id);
                } catch (Exception e2) {
                    // Don't mask the original exception but log it
                    _log.error("Failed to mark unsuccessfully started migration as canceled: [id={}]", id, e2);
                }
            }

            throw Throwables.propagate(e);
        }
    }

    public ScanStatus getStatus(String id) {
        return _statusDAO.getScanStatus(id);
    }

    /**
     * Sometimes due to unexpected errors while submitting migrator ranges to the underlying queues a migration can get stuck.
     * This method takes all available tasks for a migration and resubmits them.  This method is safe because
     * the underlying system is resilient to task resubmissions and concurrent work on the same task.
     */
    public ScanStatus resubmitWorkflowTasks(String migrationId) {
        ScanStatus status = _statusDAO.getScanStatus(migrationId);
        if (status == null) {
            return null;
        }

        if (status.getCompleteTime() == null) {
            // Resubmit any active tasks
            for (ScanRangeStatus active : status.getActiveScanRanges()) {
                _workflow.addScanRangeTask(migrationId, active.getTaskId(), active.getPlacement(), active.getScanRange());
            }

            // Send notification to evaluate whether any new range tasks can be started
            _workflow.scanStatusUpdated(migrationId);
        }

        return status;
    }

    public void cancel(String id) {
        _statusDAO.setCanceled(id);

        // Notify the workflow the migrator status was updated
        _workflow.scanStatusUpdated(id);

    }
}
