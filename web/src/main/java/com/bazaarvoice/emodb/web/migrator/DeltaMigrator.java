package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.db.ScanRangeSplits;
import com.bazaarvoice.emodb.web.migrator.migratorstatus.MigratorStatus;
import com.bazaarvoice.emodb.web.migrator.migratorstatus.MigratorStatusDAO;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.control.ScanPlan;
import com.bazaarvoice.emodb.web.scanner.control.ScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanRangeStatus;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class DeltaMigrator {

    private static final Logger _log = LoggerFactory.getLogger(DeltaMigrator.class);

    private final DataTools _dataTools;
    private final MigratorStatusDAO _statusDAO;
    private final ScanWorkflow _workflow;
    private final int _defaultMaxConcurrentWrites;

    @Inject
    public DeltaMigrator(DataTools dataTools, MigratorStatusDAO statusDAO, ScanWorkflow workflow,
                         @Named ("maxConcurrentWrites") Integer maxConcurrentWrites) {
        _dataTools = checkNotNull(dataTools, "dataTools");
        _statusDAO = checkNotNull(statusDAO, "statusDAO");
        _workflow = checkNotNull(workflow, "workflow");
        _defaultMaxConcurrentWrites = maxConcurrentWrites;

    }

    public MigratorStatus migratePlacement(String placement, int maxConcurrentWrites) {
        ScanOptions options = new ScanOptions(placement)
                .setScanByAZ(true);
        MigratorPlan plan = createPlan(placement, options);
        MigratorStatus status = plan.toMigratorStatus(maxConcurrentWrites > 0 ? maxConcurrentWrites: _defaultMaxConcurrentWrites);

        startMigration(placement, status);

        return status;

    }

    private MigratorPlan createPlan(String id, ScanOptions options) {
        MigratorPlan plan = new MigratorPlan(id, options);

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

    private void startMigration(String placement, MigratorStatus status) {
        boolean migrationCreated = false;

        try {
            // Create the migration
            _statusDAO.updateMigratorStatus(status);
            migrationCreated = true;

            // Notify the workflow that the migration can be started
            _workflow.scanStatusUpdated(placement);

        } catch (Exception e) {
            _log.error("Failed to start migration for {}", placement, e);

            if (migrationCreated) {
                // The migrator was not properly started; cancel the migration
                try {
                    _statusDAO.setCanceled(placement);
                } catch (Exception e2) {
                    // Don't mask the original exception but log it
                    _log.error("Failed to mark unsuccessfully started migration as canceled: [id={}]", placement, e2);
                }
            }

            throw Throwables.propagate(e);
        }
    }

    public MigratorStatus getStatus(String placement) {
        return _statusDAO.getMigratorStatus(placement);
    }

    /**
     * Sometimes due to unexpected errors while submitting migrator ranges to the underlying queues a migration can get stuck.
     * This method takes all available tasks for a migration and resubmits them.  This method is safe because
     * the underlying system is resilient to task resubmissions and concurrent work on the same task.
     */
    public MigratorStatus resubmitWorkflowTasks(String placement) {
        MigratorStatus status = _statusDAO.getMigratorStatus(placement);
        if (status == null) {
            return null;
        }

        if (status.getCompleteTime() == null) {
            // Resubmit any active tasks
            for (ScanRangeStatus active : status.getActiveScanRanges()) {
                _workflow.addScanRangeTask(placement, active.getTaskId(), active.getPlacement(), active.getScanRange());
            }

            // Send notification to evaluate whether any new range tasks can be started
            _workflow.scanStatusUpdated(placement);
        }

        return status;
    }

    public void cancel(String id) {
        _statusDAO.setCanceled(id);

        // Notify the workflow the migrator status was updated
        _workflow.scanStatusUpdated(id);

    }

    public void throttle(String placement, int maxConcurrentWrites) {
        _statusDAO.setMaxConcurrentWrites(placement, maxConcurrentWrites);
    }

    private class MigratorPlan extends ScanPlan {

        public MigratorPlan(String placement, ScanOptions options) {
            super(placement, options);
        }

        public MigratorStatus toMigratorStatus(int maxConcurrentWrites) {
            return new MigratorStatus(toScanStatus(), maxConcurrentWrites);
        }
    }
}
