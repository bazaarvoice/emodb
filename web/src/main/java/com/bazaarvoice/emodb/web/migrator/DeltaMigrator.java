package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
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
    private final StashStateListener _listener;
    private final int _defaultMaxConcurrentWrites;

    @Inject
    public DeltaMigrator(DataTools dataTools, MigratorStatusDAO statusDAO, ScanWorkflow workflow,
                         StashStateListener listener, @Named ("maxConcurrentWrites") Integer maxConcurrentWrites) {
        _dataTools = checkNotNull(dataTools, "dataTools");
        _statusDAO = checkNotNull(statusDAO, "statusDAO");
        _workflow = checkNotNull(workflow, "workflow");
        _listener = checkNotNull(listener, "listener");
        _defaultMaxConcurrentWrites = maxConcurrentWrites;

    }

    public MigratorStatus migratePlacement(String placement, String migrationId) {
        ScanOptions options = new ScanOptions(placement)
                .setScanByAZ(true);
        MigratorPlan plan = createPlan(migrationId, options);
        MigratorStatus status = plan.toMigratorStatus();

        startMigration(migrationId, status);

        return status;

    }

    private MigratorPlan createPlan(String id, ScanOptions options) {
        MigratorPlan plan = new MigratorPlan(id, options, _defaultMaxConcurrentWrites);

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

    private void startMigration(String id, MigratorStatus status) {
        boolean migrationCreated = false;

        try {
            // Create the migration
            _statusDAO.updateMigratorStatus(status);
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

    public MigratorStatus getStatus(String id) {
        return _statusDAO.getMigratorStatus(id);
    }

    /**
     * Sometimes due to unexpected errors while submitting migrator ranges to the underlying queues a migration can get stuck.
     * This method takes all available tasks for a migration and resubmits them.  This method is safe because
     * the underlying system is resilient to task resubmissions and concurrent work on the same task.
     */
    public MigratorStatus resubmitWorkflowTasks(String migrationId) {
        MigratorStatus status = _statusDAO.getMigratorStatus(migrationId);
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

    private class MigratorPlan extends ScanPlan {
        private int _maxConcurrentWrites;

        public MigratorPlan(String migrationId, ScanOptions options, int maxConcurrentWrites) {
            super(migrationId, options);
            _maxConcurrentWrites = maxConcurrentWrites;
        }

        public MigratorStatus toMigratorStatus() {
            return new MigratorStatus(toScanStatus(), _maxConcurrentWrites);
        }
    }
}
