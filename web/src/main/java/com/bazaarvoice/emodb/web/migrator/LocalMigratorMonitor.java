package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.db.ScanRangeSplits;
import com.bazaarvoice.emodb.web.migrator.migratorstatus.MigratorStatus;
import com.bazaarvoice.emodb.web.migrator.migratorstatus.MigratorStatusDAO;
import com.bazaarvoice.emodb.web.scanner.control.ScanRangeComplete;
import com.bazaarvoice.emodb.web.scanner.control.ScanRangeTask;
import com.bazaarvoice.emodb.web.scanner.control.ScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.notifications.ScanCountListener;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanRangeStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.google.common.base.Optional;
import com.google.common.collect.*;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class LocalMigratorMonitor extends AbstractService {

    private static final Duration OVERRUN_MIGRATION_TIME = Duration.standardDays(1);


    private final Logger _log = LoggerFactory.getLogger(LocalMigratorMonitor.class);


    private final ScanWorkflow _workflow;
    private final MigratorStatusDAO _statusDAO;
    private final StashStateListener _migratorStateListener;
    private final ScanCountListener _migratorCountListener;
    private final DataTools _dataTools;
    private final Set<String> _activeMigrations = Sets.newHashSet();

    private ScheduledExecutorService _service;

    public LocalMigratorMonitor(ScanWorkflow scanWorkflow, MigratorStatusDAO scanStatusDAO, StashStateListener migratorStateListener,
                                ScanCountListener migratorCountListener, DataTools dataTools) {
        _workflow = checkNotNull(scanWorkflow, "scanWorkflow");
        _statusDAO = checkNotNull(scanStatusDAO, "scanStatusDAO");
        _migratorStateListener = checkNotNull(migratorStateListener, "migratorStateListener");
        _migratorCountListener = checkNotNull(migratorCountListener, "scanCountListener");
        _dataTools = checkNotNull(dataTools, "dataTools");
    }

    @Override
    protected void doStart() {
        _log.info("Starting migrator monitor service");

        if (_service == null) {
            _service = Executors.newScheduledThreadPool(1,
                    new ThreadFactoryBuilder().setNameFormat("migrator-monitor-%d").build());
        }

        _service.execute(() -> {
            try {
                // Initialize active migrations with migrations that were already active when this instance became leader
                initializeAllActiveMigrations();

                // Set the starting count for the migration count notifier
                notifyActiveMigrationCountChanged();

                // Start the loop for processing complete range migrations
                _service.schedule(_processCompleteRangeMigrationsExecution, 1, TimeUnit.SECONDS);

                notifyStarted();

            } catch (Exception e) {
                _log.error("Failed to start local migration upload monitor", e);
                notifyFailed(e);
            }
        });
    }

    @Override
    protected void doStop() {
        _log.info("Stopping migrator monitor service");

        if (_service != null) {
            _service.shutdownNow();
            try {
                if (!_service.awaitTermination(30, TimeUnit.SECONDS)) {
                    _log.warn("Service still has running threads after shutdown request");
                }
            } catch (InterruptedException e) {
                _log.warn("Service interrupted while waiting for shutdown", e);
            }
            _service = null;
        }

        // Send a final notification that the active count is zero; whoever picks up leadership going forward
        // will take over active migration count notifications.
        _activeMigrations.clear();
        notifyActiveMigrationCountChanged();

        notifyStopped();
    }

    private final Runnable _processCompleteRangeMigrationsExecution = new Runnable() {
        @Override
        public void run() {
            try {
                processCompleteRangeScans();
            } catch (Exception e) {
                // This should never happen; all exceptions should already be caught in processCompleteRangeScans()
                _log.error("Unexpected exception caught processing complete range migrations", e);
            }

            // Schedule the next check depending on whether any scans are active.
            long delaySeconds = _activeMigrations.isEmpty() ? 3 : 1;

            // Repeat execution after the previously computed delay
            _service.schedule(_processCompleteRangeMigrationsExecution, delaySeconds, TimeUnit.SECONDS);
        }
    };

    protected void processCompleteRangeScans() {
        Multimap<String, ScanRangeComplete> completeRangeMigrationsByScanId;

        try {
            completeRangeMigrationsByScanId = Multimaps.index(
                    _workflow.claimCompleteScanRanges(Duration.standardMinutes(5)),
                    completion -> completion.getScanId());
        } catch (Exception e) {
            _log.error("Failed to claim complete migration ranges", e);
            return;
        }

        for (Map.Entry<String, Collection<ScanRangeComplete>> entry : completeRangeMigrationsByScanId.asMap().entrySet()) {
            String scanId = entry.getKey();
            Collection<ScanRangeComplete> completions = entry.getValue();

            try {
                refreshMigration(scanId);
                _workflow.releaseCompleteScanRanges(completions);
            } catch (Exception e) {
                _log.error("Failed to process migration range complete: id={}", scanId, e);
            }
        }
    }

    private void refreshMigration(String id)
            throws IOException {
        ScanStatus status = _statusDAO.getMigratorStatus(id);
        if (status == null) {
            _log.warn("Refresh migration called for unknown scan: {}", id);
            return;
        }

        if (status.isDone()) {
            if (status.isCanceled()) {
                migrationCanceled(status);
            } else {
                completeMigration(status);
            }
            return;
        }

        // Update the set of active migrations
        if (_activeMigrations.add(id)) {
            // This is a new migration.  Notify that the number of active migrations has changed.
            notifyActiveMigrationCountChanged();
            // Schedule a callback to cancel the migration if it goes overrun
            scheduleOverrunCheck(status);
        }

        // Before evaluating available tasks check whether any completed tasks didn't scan their entire ranges
        // and require the addition of new tasks.
        status = resplitPartiallyCompleteTasks(status);

        Set<Integer> incompleteBatches = getIncompleteBatches(status);
        Multimap<Integer, ScanRangeStatus> queuedScansByConcurrencyId = getQueuedRangeMigrationsByConcurrencyId(status);
        int maxConcurrency = status.getOptions().getMaxConcurrentSubRangeScans();
        Date now = new Date();

        for (ScanRangeStatus rangeStatus : getUnqueuedRangeMigrations(status)) {
            Optional<Integer> blockedByBatchId = rangeStatus.getBlockedByBatchId();
            Optional<Integer> concurrencyId = rangeStatus.getConcurrencyId();

            // Queue up the pending range migration if the following conditions are both met:
            // 1. It has no blocking batch or the blocking batch is complete
            // 2. The maximum concurrency permitted has not already been met
            if ((!blockedByBatchId.isPresent() || !incompleteBatches.contains(blockedByBatchId.get())) &&
                    (!concurrencyId.isPresent() || queuedScansByConcurrencyId.get(concurrencyId.get()).size() < maxConcurrency)) {
                int taskId = rangeStatus.getTaskId();
                String placement = rangeStatus.getPlacement();
                ScanRange range = rangeStatus.getScanRange();
                ScanRangeTask task = _workflow.addScanRangeTask(id, taskId, placement, range);
                _statusDAO.setMigratorRangeTaskQueued(id, taskId, now);

                if (concurrencyId.isPresent()) {
                    // Mark that this range has been queued so this loop doesn't over-add range migrations
                    queuedScansByConcurrencyId.put(concurrencyId.get(), rangeStatus);
                }

                _log.info("Queued migration range task: {}", task);
            }
        }
    }

    /**
     * Checks whether any completed tasks returned before migrating the entire range.  If so then the unmigrated
     * ranges are resplit and new tasks are created from them.
     */
    private ScanStatus resplitPartiallyCompleteTasks(ScanStatus status) {
        boolean anyUpdated = false;
        int nextTaskId = -1;

        for (ScanRangeStatus complete : status.getCompleteScanRanges()) {
            if (complete.getResplitRange().isPresent()) {
                // This task only partially completed; there are still more data to migrate.
                if (nextTaskId == -1) {
                    nextTaskId = getNextTaskId(status);
                }

                ScanRange resplitRange = complete.getResplitRange().get();

                // Resplit the un-migrated portion into new ranges
                List<ScanRange> subRanges = resplit(complete.getPlacement(), resplitRange, status.getOptions().getRangeScanSplitSize());
                // Create new tasks for each subrange that are immediately available for being queued.
                List<ScanRangeStatus> resplitStatuses = Lists.newArrayListWithCapacity(subRanges.size());
                for (ScanRange subRange : subRanges) {
                    resplitStatuses.add(
                            new ScanRangeStatus(nextTaskId++, complete.getPlacement(), subRange,
                                    complete.getBatchId(), complete.getBlockedByBatchId(), complete.getConcurrencyId()));
                }

                _statusDAO.resplitMigratorRangeTask(status.getScanId(), complete.getTaskId(), resplitStatuses);

                anyUpdated = true;
            }
        }

        if (!anyUpdated) {
            return status;
        }

        // Slightly inefficient to reload but less risky than trying to keep the DAO and in-memory object in sync
        return _statusDAO.getMigratorStatus(status.getScanId());
    }

    private List<ScanRange> resplit(String placement, ScanRange resplitRange, int splitSize) {
        // Cassandra didn't do a good job splitting the first time.  Re-split the remaining range; the issue that causes
        // the poor split is rare and it should do a better job this time.
        ScanRangeSplits splits = _dataTools.getScanRangeSplits(placement, splitSize, Optional.of(resplitRange));
        ImmutableList.Builder<ScanRange> builder = ImmutableList.builder();
        for (ScanRangeSplits.SplitGroup splitGroup : splits.getSplitGroups()) {
            for (ScanRangeSplits.TokenRange tokenRange : splitGroup.getTokenRanges()) {
                builder.addAll(tokenRange.getScanRanges());
            }
        }
        return builder.build();
    }

    private Set<Integer> getIncompleteBatches(ScanStatus status) {
        Iterable<ScanRangeStatus> incompleteRangeScans = Iterables.concat(status.getPendingScanRanges(), status.getActiveScanRanges());
        return FluentIterable.from(incompleteRangeScans)
                .transform(rangeStatus -> rangeStatus.getBatchId())
                .toSet();
    }

    private Multimap<Integer, ScanRangeStatus> getQueuedRangeMigrationsByConcurrencyId(ScanStatus status) {
        Iterable<ScanRangeStatus> incompleteRangeMigrations = Iterables.concat(status.getPendingScanRanges(), status.getActiveScanRanges());
        Multimap<Integer, ScanRangeStatus> queuedMigrationsByConcurrencyId = ArrayListMultimap.create();
        for (ScanRangeStatus rangeStatus : incompleteRangeMigrations) {
            if (hasBeenQueued(rangeStatus) && rangeStatus.getConcurrencyId().isPresent()) {
                queuedMigrationsByConcurrencyId.put(rangeStatus.getConcurrencyId().get(), rangeStatus);
            }
        }
        return queuedMigrationsByConcurrencyId;
    }

    public List<ScanRangeStatus> getUnqueuedRangeMigrations(ScanStatus status) {
        // Analyze unqueued migration ranges by in migration-range order so sub-ranges will be queued deterministically
        return FluentIterable.from(status.getPendingScanRanges())
                .filter(rangeStatus -> !hasBeenQueued(rangeStatus))
                .toSortedList(new Comparator<ScanRangeStatus>() {
                    @Override
                    public int compare(ScanRangeStatus status1, ScanRangeStatus status2) {
                        return ComparisonChain.start()
                                .compare(status1.getBatchId(), status2.getBatchId())
                                .compare(status1.getPlacement(), status2.getPlacement())
                                .compare(status1.getScanRange(), status2.getScanRange())
                                .result();
                    }
                });
    }

    private boolean hasBeenQueued(ScanRangeStatus rangeStatus) {
        return rangeStatus.getScanQueuedTime() != null;
    }

    private int getNextTaskId(ScanStatus status) {
        Iterable<ScanRangeStatus> scanRangeStatuses = status.getAllScanRanges();
        if (Iterables.isEmpty(scanRangeStatuses)) {
            return 0;
        }

        return Ordering.natural().max(
                FluentIterable.from(scanRangeStatuses)
                        .transform(scanRangeStatus -> scanRangeStatus.getTaskId())) + 1;
    }

    private void migrationCanceled(ScanStatus status) {
        // Send notification that the migration has been canceled
        _migratorStateListener.stashCanceled(status.asPluginStashMetadata(), new Date());
        cleanupMigration(status.getScanId());
    }

    private void completeMigration(ScanStatus status)
            throws IOException {
        String id = status.getScanId();
        if (status.getCompleteTime() != null) {
            _log.info("Migration already marked complete: {}", id);
            return;
        }

        _log.info("Migration complete: {}", id);

        try {

            // Store the time the migration completed
            Date completeTime = new Date();
            _statusDAO.setCompleteTime(id, completeTime);

            // Send notification that the migration has completed.
            status.setCompleteTime(completeTime);
            _migratorStateListener.stashCompleted(status.asPluginStashMetadata(), status.getCompleteTime());
        } finally {
            cleanupMigration(id);
        }
    }

    private void cleanupMigration(String id) {
        // Remove this migration from the active set
        if (_activeMigrations.remove(id)) {
            notifyActiveMigrationCountChanged();
        }

    }

    private void initializeAllActiveMigrations() {
        try {
            Iterator<MigratorStatus> statuses = _statusDAO.list(null, Long.MAX_VALUE);
            while (statuses.hasNext()) {
                ScanStatus status = statuses.next();
                if (!status.isDone()) {
                    // Record that the migration is active
                    _activeMigrations.add(status.getScanId());
                    // Schedule a callback to cancel the migration if it goes overrun
                    scheduleOverrunCheck(status);
                }
            }
        } catch (Exception e) {
            // This is bad but not critical; if there are any active migrations they will likely check in with a completed
            // migration range within several minutes anyway.
            _log.warn("Failed to initialize active migration count", e);
        }
    }

    private void scheduleOverrunCheck(ScanStatus status) {
        final String migratorId = status.getScanId();

        DateTime now = DateTime.now();
        DateTime overrunTime = new DateTime(status.getStartTime()).plus(OVERRUN_MIGRATION_TIME);

        long delay = 0;
        if (now.isBefore(overrunTime)) {
            delay = new Duration(now, overrunTime).getMillis();
        }

        _service.schedule(() -> {
                    ScanStatus migratorStatus = _statusDAO.getMigratorStatus(migratorId);
                    if (!migratorStatus.isDone()) {
                        _log.warn("Overrun migration detected, canceling migration: {}", migratorId);
                        _statusDAO.setCanceled(migratorId);
                        _workflow.scanStatusUpdated(migratorId);

                    }
                },
                delay, TimeUnit.MILLISECONDS);
    }

    private void notifyActiveMigrationCountChanged() {
        _migratorCountListener.activeScanCountChanged(_activeMigrations.size());
    }
}
