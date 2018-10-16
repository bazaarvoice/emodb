package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.web.migrator.migratorstatus.MigratorStatusDAO;
import com.bazaarvoice.emodb.web.scanner.control.MaxConcurrentScans;
import com.bazaarvoice.emodb.web.scanner.control.ScanRangeTask;
import com.bazaarvoice.emodb.web.scanner.control.ScanWorkflow;
import com.bazaarvoice.emodb.web.scanner.rangescan.RangeScanUploaderResult;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanRangeStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class DistributedMigratorRangeMonitor implements Managed {
    // TTL for the initially claiming items off the workflow queue
    private static final Duration QUEUE_CLAIM_TTL = Duration.ofMinutes(2);
    // TTL for renewing workflow queue items once they have started
    private static final Duration QUEUE_RENEW_TTL = Duration.ofMinutes(4);
    // Time after which a claimed task should be unclaimed if it has not started
    private static final Duration CLAIM_START_TIMEOUT = QUEUE_CLAIM_TTL.minus(Duration.ofSeconds(15));
    private final Logger _log = LoggerFactory.getLogger(DistributedMigratorRangeMonitor.class);
    private final ScanWorkflow _workflow;
    private final MigratorStatusDAO _statusDAO;
    private final LocalRangeMigrator _rangeMigrator;
    private final int _maxConcurrentMigrations;

    private final ConcurrentMap<Integer, ClaimedTask> _claimedTasks = Maps.newConcurrentMap();
    private ExecutorService _migratingService;
    private ScheduledExecutorService _backgroundService;
    private final Runnable _startMigrationsIfAvailableRunnable = new Runnable() {
        @Override
        public void run() {
            startMigrationsIfAvailable();
        }
    };

    @Inject
    public DistributedMigratorRangeMonitor(ScanWorkflow workflow, MigratorStatusDAO statusDAO, LocalRangeMigrator rangeMigrator, @MaxConcurrentScans int maxConcurrentMigrations, LifeCycleRegistry lifecycle) {
        _workflow = checkNotNull(workflow, "workflow");
        _statusDAO = checkNotNull(statusDAO, "statusDAO");
        _rangeMigrator = rangeMigrator;
        _maxConcurrentMigrations = maxConcurrentMigrations;

        lifecycle.manage(this);
    }

    @Override
    public void start() throws Exception {
        _log.info("Distributed migrator range monitor is starting");

        if (_migratingService == null) {
            _migratingService = Executors.newFixedThreadPool(_maxConcurrentMigrations, new ThreadFactoryBuilder().setNameFormat("MigratorRange-%d").build());
        }

        if (_backgroundService == null) {
            _backgroundService = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("MigratorRangeRenewal-%d").build());
        }

        // Schedule a check for new migration tasks
        _backgroundService.scheduleWithFixedDelay(_startMigrationsIfAvailableRunnable, 5, 5, TimeUnit.SECONDS);

        // Schedule claim renewals for active scans
        _backgroundService.scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        renewClaimedTasks();
                    }
                }, 1, 1, TimeUnit.MINUTES
        );


    }

    @Override
    public void stop() throws Exception {
        _log.info("Migrator monitor is stopping");

        if (_backgroundService != null) {
            _backgroundService.shutdownNow();
            _backgroundService = null;
        }
        if (_migratingService != null) {
            _migratingService.shutdownNow();
            _migratingService = null;
        }
    }

    public void startMigrationsIfAvailable() {
        try {
            int availableCount;

            while ((availableCount = getAvailableMigrationThreadCount()) > 0) {
                List<ClaimedTask> claimedTasks = claimMigrationRangeTasks(availableCount);

                if (claimedTasks.isEmpty()) {
                    // No tasks available; wait until the next iteration to try again
                    return;
                }

                for (final ClaimedTask claimedTask : claimedTasks) {
                    // Start the migration asynchronously
                    _migratingService.submit(() -> executeClaimedTask(claimedTask));
                }
            }
        } catch (Exception e) {
            _log.error("Failed to start available migrations", e);
        }
    }

    /**
     * Returns the number of threads currently available to start a migration.
     */
    private int getAvailableMigrationThreadCount() {
        return _maxConcurrentMigrations - _claimedTasks.size();
    }

    /**
     * Claims migration range tasks that have been queued by the leader and are ready to scan.
     */
    private List<ClaimedTask> claimMigrationRangeTasks(int max) {
        try {
            Date claimTime = new Date();
            List<ScanRangeTask> migrationRangeTasks = _workflow.claimScanRangeTasks(max, QUEUE_CLAIM_TTL);
            if (migrationRangeTasks.isEmpty()) {
                return ImmutableList.of();
            }

            List<ClaimedTask> newlyClaimedTasks = Lists.newArrayListWithCapacity(migrationRangeTasks.size());

            for (ScanRangeTask task : migrationRangeTasks) {
                final ClaimedTask claimedTask = new ClaimedTask(task, claimTime);

                // Record that the task is claimed locally
                boolean alreadyClaimed = _claimedTasks.putIfAbsent(task.getId(), claimedTask) != null;
                if (alreadyClaimed) {
                    _log.warn("Workflow returned migration range task that is already claimed: {}", task);
                    // Do not acknowledge the task, let it expire naturally.  Eventually it should come up again
                    // after the previous claim has been released.
                } else {
                    _log.info("Claimed migration range task: {}", task);
                    newlyClaimedTasks.add(claimedTask);

                    // Schedule a follow-up to ensure the scanning service assigns it a thread
                    // in a reasonable amount of time.
                    _backgroundService.schedule(
                            new Runnable() {
                                @Override
                                public void run() {
                                    validateClaimedTaskHasStarted(claimedTask);
                                }
                            },
                            CLAIM_START_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                }
            }

            return newlyClaimedTasks;
        } catch (Exception e) {
            _log.error("Failed to start next available migration range", e);
            return ImmutableList.of();
        }
    }

    private void validateClaimedTaskHasStarted(ClaimedTask claimedTask) {
        // This will return false if the task has already started
        if (claimedTask.unclaimIfNotStarted()) {
            _log.warn("Claimed task has not started since it was scheduled at {}; unclaiming task: {}",
                    claimedTask.getClaimTime(), claimedTask.getTask());

            unclaimTask(claimedTask, false);
        }
    }

    private void unclaimTask(ClaimedTask claimedTask, boolean releaseTask) {
        _claimedTasks.remove(claimedTask.getTaskId());
        claimedTask.setComplete(true);

        if (releaseTask) {
            try {
                _workflow.releaseScanRangeTask(claimedTask.getTask());
                _log.info("Released migration range task: {}", claimedTask.getTask());
            } catch (Exception e) {
                _log.error("Failed to release migration range", e);
            }
        }
    }

    private void renewClaimedTasks() {
        try {
            List<ClaimedTask> claimedTasks = ImmutableList.copyOf(_claimedTasks.values());
            List<ScanRangeTask> tasks = Lists.newArrayList();

            for (ClaimedTask claimedTask : claimedTasks) {
                if (claimedTask.isComplete()) {
                    // Task is likely being removed in another thread.  However, go ahead and remove it now
                    // to allow other tasks to start sooner.
                    _log.info("Complete claimed task found during renew: id={}", claimedTask.getTaskId());
                    _claimedTasks.remove(claimedTask.getTaskId());
                } else if (claimedTask.isStarted()) {
                    // Task has started and is not complete.  Renew it.
                    tasks.add(claimedTask.getTask());
                }
            }

            if (!tasks.isEmpty()) {
                _workflow.renewScanRangeTasks(tasks, QUEUE_RENEW_TTL);
                for (ScanRangeTask task : tasks) {
                    _log.info("Renewed migration range task: {}", task);
                }
            }
        } catch (Exception e) {
            _log.error("Failed to renew migration ranges", e);
        }
    }

    /**
     * Executes a previously claimed scan range task.
     */
    private void executeClaimedTask(ClaimedTask claimedTask) {
        // Check whether this claim was already abandoned due to late execution.
        if (!claimedTask.setStartTime(new Date())) {
            _log.info("Claimed task is overdue; range not scanned: {}", claimedTask.getTask());
            return;
        }

        ScanRangeTask task = claimedTask.getTask();
        boolean releaseTask = false;
        try {
            // Immediately renew the claim; future renewals will be handled asynchronously by the background renewal task.
            _workflow.renewScanRangeTasks(ImmutableList.of(task), QUEUE_RENEW_TTL);

            releaseTask = rangeMigration(task);
        } finally {
            unclaimTask(claimedTask, releaseTask);

            // Immediately try to claim a new scan when this one finishes
            _backgroundService.submit(_startMigrationsIfAvailableRunnable);
        }
    }

    private boolean rangeMigration(ScanRangeTask task) {
        final String migrationId = task.getScanId();
        final int taskId = task.getId();
        final String placement = task.getPlacement();
        final ScanRange range = task.getRange();
        RangeScanUploaderResult result;

        try {
            // Verify that this range hasn't already been completed (protect against queue re-posts)
            ScanStatus status = _statusDAO.getMigratorStatus(migrationId);

            if (status.isCanceled()) {
                _log.info("Ignoring migration range from canceled task: [task={}]", task);
                return true;
            }

            ScanRangeStatus completedStatus = Iterables.getOnlyElement(
                    Iterables.filter(status.getCompleteScanRanges(), new Predicate<ScanRangeStatus>() {
                        @Override
                        public boolean apply(ScanRangeStatus rangeStatus) {
                            return rangeStatus.getTaskId() == taskId;
                        }
                    }), null);

            if (completedStatus != null) {
                _log.info("Ignoring duplicate post of completed migration range task: [task={}, completeTime={}]",
                        task, completedStatus.getScanCompleteTime());
                return true;
            }

            while (!_rangeMigrator.isCapableOfMigrating(placement)) {
                _log.warn("FCL is too high. Delta migration will block until it comes down.");
                Thread.sleep(60 * 1000);
            }

            _log.info("Started migration range task: {}", task);

            _statusDAO.setMigratorRangeTaskActive(migrationId, taskId, new Date());

            result = _rangeMigrator.migrate(taskId, status.getOptions(), placement, range, migrationId);


            _log.info("Completed migration range task: {}", task);
        } catch (Throwable t) {
            _log.error("Migration range task failed: {}", task, t);
            result = RangeScanUploaderResult.failure();
        }

        try {
            switch (result.getStatus()) {
                case SUCCESS:
                    _statusDAO.setMigratorRangeTaskComplete(migrationId, taskId, new Date());
                    break;
                case FAILURE:
                    _statusDAO.setMigratorRangeTaskInactive(migrationId, taskId);
                    break;
                case REPSPLIT:
                    // The portion of the range up to the resplit is what was completed.
                    //noinspection ConstantConditions
                    ScanRange completedRange = ScanRange.create(range.getFrom(), result.getResplitRange().getFrom());
                    _statusDAO.setMigratorRangeTaskPartiallyComplete(migrationId, taskId, completedRange, result.getResplitRange(), new Date());
                    break;
            }
        } catch (Throwable t) {
            _log.error("Failed to mark migration range result: [id={}, placement={}, range={}, result={}]",
                    migrationId, placement, range, result, t);

            // Since the scan result wasn't marked the leader will not have an accurate view of the state.
            return false;
        }

        return true;
    }

    /**
     * Maintains metadata for a claimed ScanRangeTask
     */
    private class ClaimedTask {
        private final ScanRangeTask _task;
        private final Date _claimTime;
        private Date _startTime;
        private boolean _allowStart = true;
        private boolean _complete = false;

        private ClaimedTask(ScanRangeTask task, Date claimTime) {
            _task = task;
            _claimTime = claimTime;
        }

        private int getTaskId() {
            return _task.getId();
        }

        private ScanRangeTask getTask() {
            return _task;
        }

        private Date getClaimTime() {
            return _claimTime;
        }

        private synchronized boolean setStartTime(Date startTime) {
            if (!_allowStart) {
                return false;
            }
            _startTime = startTime;
            return true;
        }

        private synchronized boolean unclaimIfNotStarted() {
            if (_startTime != null) {
                return false;
            }
            _allowStart = false;
            return true;
        }

        private boolean isStarted() {
            return _startTime != null;
        }

        private boolean isComplete() {
            return _complete;
        }

        private void setComplete(boolean complete) {
            _complete = complete;
        }

        @Override
        public boolean equals(Object other) {
            return other == this ||
                    (other instanceof ClaimedTask && _task.equals(((ClaimedTask) other)._task));
        }

        @Override
        public int hashCode() {
            return _task.hashCode();
        }
    }
}