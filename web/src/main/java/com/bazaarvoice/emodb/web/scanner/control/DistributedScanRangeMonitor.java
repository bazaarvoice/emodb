package com.bazaarvoice.emodb.web.scanner.control;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.web.scanner.rangescan.RangeScanUploader;
import com.bazaarvoice.emodb.web.scanner.rangescan.RangeScanUploaderResult;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanRangeStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatusDAO;
import com.google.common.annotations.VisibleForTesting;
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

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Managed process which listens for scan ranges ready to be uploaded via a previous call to
 * {@link ScanWorkflow#addScanRangeTask(String, int, String, com.bazaarvoice.emodb.sor.db.ScanRange)}.  This process
 * accepts available upload tasks and performs them locally.
 */
public class DistributedScanRangeMonitor implements Managed {

    private final Logger _log = LoggerFactory.getLogger(DistributedScanRangeMonitor.class);

    // TTL for the initially claiming items off the workflow queue
    private static final Duration QUEUE_CLAIM_TTL = Duration.ofMinutes(2);
    // TTL for renewing workflow queue items once they have started
    private static final Duration QUEUE_RENEW_TTL = Duration.ofMinutes(4);
    // Time after which a claimed task should be unclaimed if it has not started
    private static final Duration CLAIM_START_TIMEOUT = QUEUE_CLAIM_TTL.minus(Duration.ofSeconds(15));

    private final ScanWorkflow _scanWorkflow;
    private final ScanStatusDAO _scanStatusDAO;
    private final RangeScanUploader _rangeScanUploader;
    private final int _maxConcurrentScans;
    // No ConcurrentSet in Java, so use a ConcurrentMap instead.
    private final ConcurrentMap<Integer, ClaimedTask> _claimedTasks = Maps.newConcurrentMap();
    private ExecutorService _scanningService;
    private ScheduledExecutorService _backgroundService;

    @Inject
    public DistributedScanRangeMonitor(ScanWorkflow scanWorkflow, ScanStatusDAO scanStatusDAO,
                                       RangeScanUploader rangeScanUploader,
                                       @MaxConcurrentScans int maxConcurrentScans, LifeCycleRegistry lifecycle) {
        _scanWorkflow = requireNonNull(scanWorkflow, "scanWorkflow");
        _scanStatusDAO = requireNonNull(scanStatusDAO, "scanStatusDAO");
        _rangeScanUploader = requireNonNull(rangeScanUploader, "rangeScanUploader");
        checkArgument(maxConcurrentScans > 0, "maxConcurrentScans <= 0");
        _maxConcurrentScans = maxConcurrentScans;

        lifecycle.manage(this);
    }

    @VisibleForTesting
    public void setExecutorServices(ExecutorService scanningService, ScheduledExecutorService backgroundService) {
        _scanningService = scanningService;
        _backgroundService = backgroundService;
    }

    @Override
    public void start()
            throws Exception {
        _log.info("Distributed scan range monitor is starting");

        if (_scanningService == null) {
            _scanningService = Executors.newFixedThreadPool(_maxConcurrentScans, new ThreadFactoryBuilder().setNameFormat("ScanRange-%d").build());
        }
        if (_backgroundService == null) {
            _backgroundService = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("ScanRangeRenewal-%d").build());
        }

        // Schedule a check for new scan range tasks
        _backgroundService.scheduleWithFixedDelay(_startScansIfAvailableRunnable, 5, 5, TimeUnit.SECONDS);

        // Schedule claim renewals for active scans
        _backgroundService.scheduleWithFixedDelay(
                new Runnable() {
                    @Override
                    public void run() {
                        renewClaimedTasks();
                    }
                },
                1, 1, TimeUnit.MINUTES);
    }

    @Override
    public void stop()
            throws Exception {
        _log.info("Distributed scan range monitor is stopping");

        if (_backgroundService != null) {
            _backgroundService.shutdownNow();
            _backgroundService = null;
        }
        if (_scanningService != null) {
            _scanningService.shutdownNow();
            _scanningService = null;
        }
    }

    private final Runnable _startScansIfAvailableRunnable = new Runnable() {
        @Override
        public void run() {
            startScansIfAvailable();
        }
    };

    @VisibleForTesting
    public void startScansIfAvailable() {
        try {
            int availableCount;

            while ((availableCount = getAvailableScanThreadCount()) > 0) {
                List<ClaimedTask> claimedTasks = claimScanRangeTasks(availableCount);

                if (claimedTasks.isEmpty()) {
                    // No tasks available; wait until the next iteration to try again
                    return;
                }

                for (final ClaimedTask claimedTask : claimedTasks) {
                    // Start the scan asynchronously
                    _scanningService.submit(new Runnable() {
                        @Override
                        public void run() {
                            executeClaimedTask(claimedTask);
                        }
                    });
                }
            }
        } catch (Exception e) {
            _log.error("Failed to start available scans", e);
        }
    }

    /**
     * Returns the number of threads currently available to start a scan.
     */
    private int getAvailableScanThreadCount() {
        return _maxConcurrentScans - _claimedTasks.size();
    }

    /**
     * Claims scan range tasks that have been queued by the leader and are ready to scan.
     */
    private List<ClaimedTask> claimScanRangeTasks(int max) {
        try {
            Date claimTime = new Date();
            List<ScanRangeTask> scanRangeTasks = _scanWorkflow.claimScanRangeTasks(max, QUEUE_CLAIM_TTL);
            if (scanRangeTasks.isEmpty()) {
                return ImmutableList.of();
            }

            List<ClaimedTask> newlyClaimedTasks = Lists.newArrayListWithCapacity(scanRangeTasks.size());

            for (ScanRangeTask task : scanRangeTasks) {
                final ClaimedTask claimedTask = new ClaimedTask(task, claimTime);

                // Record that the task is claimed locally
                boolean alreadyClaimed = _claimedTasks.putIfAbsent(task.getId(), claimedTask) != null;
                if (alreadyClaimed) {
                    _log.warn("Workflow returned scan range task that is already claimed: {}", task);
                    // Do not acknowledge the task, let it expire naturally.  Eventually it should come up again
                    // after the previous claim has been released.
                } else {
                    _log.info("Claimed scan range task: {}", task);
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
            _log.error("Failed to start next available scan range", e);
            return ImmutableList.of();
        }
    }

    /**
     * If we claimed a task but fail to execute it before the initial TTL then let it expire rather than holding
     * onto it for an indeterminate amount of time.
     */
    private void validateClaimedTaskHasStarted(ClaimedTask claimedTask) {
        // This will return false if the task has already started
        if (claimedTask.unclaimIfNotStarted()) {
            _log.warn("Claimed task has not started since it was scheduled at {}; unclaiming task: {}",
                    claimedTask.getClaimTime(), claimedTask.getTask());

            unclaimTask(claimedTask, false);
        }
    }

    /**
     * Renews all claimed scan range tasks that have not been released.  Unless this is called periodically the
     * scan workflow will make this task available to be claimed again.
     */
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
                _scanWorkflow.renewScanRangeTasks(tasks, QUEUE_RENEW_TTL);
                for (ScanRangeTask task : tasks) {
                    _log.info("Renewed scan range task: {}", task);
                }
            }
        } catch (Exception e) {
            _log.error("Failed to renew scan ranges", e);
        }
    }

    /**
     * Unclaims a previously claimed task.  Effectively this stops the renewing the task and, if releaseTask is true,
     * removes the task permanently from the workflow queue.
     */
    private void unclaimTask(ClaimedTask claimedTask, boolean releaseTask) {
        _claimedTasks.remove(claimedTask.getTaskId());
        claimedTask.setComplete(true);

        if (releaseTask) {
            try {
                _scanWorkflow.releaseScanRangeTask(claimedTask.getTask());
                _log.info("Released scan range task: {}", claimedTask.getTask());
            } catch (Exception e) {
                _log.error("Failed to release scan range", e);
            }
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
            _scanWorkflow.renewScanRangeTasks(ImmutableList.of(task), QUEUE_RENEW_TTL);

            releaseTask = asyncRangeScan(task);
        } finally {
            unclaimTask(claimedTask, releaseTask);

            // Immediately try to claim a new scan when this one finishes
            _backgroundService.submit(_startScansIfAvailableRunnable);
        }
    }

    /**
     * Performs a range scan and updates the global scan status with the scan result.
     * @return true if the system state is consistent and the task can be released, false otherwise
     */
    private boolean asyncRangeScan(ScanRangeTask task) {
        final String scanId = task.getScanId();
        final int taskId = task.getId();
        final String placement = task.getPlacement();
        final ScanRange range = task.getRange();
        RangeScanUploaderResult result;

        try {
            // Verify that this range hasn't already been completed (protect against queue re-posts)
            ScanStatus status = _scanStatusDAO.getScanStatus(scanId);

            if (status.isCanceled()) {
                _log.info("Ignoring scan range from canceled task: [task={}]", task);
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
                _log.info("Ignoring duplicate post of completed scan range task: [task={}, completeTime={}]",
                        task, completedStatus.getScanCompleteTime());
                return true;
            }

            _log.info("Started scan range task: {}", task);

            _scanStatusDAO.setScanRangeTaskActive(scanId, taskId, new Date());

            // Perform the range scan
            result = _rangeScanUploader.scanAndUpload(scanId, taskId, status.getOptions(), placement, range, status.getCompactionControlTime());

            _log.info("Completed scan range task: {}", task);
        } catch (Throwable t) {
            _log.error("Scan range task failed: {}", task, t);
            result = RangeScanUploaderResult.failure();
        }

        try {
            switch (result.getStatus()) {
                case SUCCESS:
                    _scanStatusDAO.setScanRangeTaskComplete(scanId, taskId, new Date());
                    break;
                case FAILURE:
                    _scanStatusDAO.setScanRangeTaskInactive(scanId, taskId);
                    break;
                case REPSPLIT:
                    // The portion of the range up to the resplit is what was completed.
                    //noinspection ConstantConditions
                    ScanRange completedRange = ScanRange.create(range.getFrom(), result.getResplitRange().getFrom());
                    _scanStatusDAO.setScanRangeTaskPartiallyComplete(scanId, taskId, completedRange, result.getResplitRange(), new Date());
                    break;
            }
        } catch (Throwable t) {
            _log.error("Failed to mark scan range result: [id={}, placement={}, range={}, result={}]",
                    scanId, placement, range, result, t);

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
