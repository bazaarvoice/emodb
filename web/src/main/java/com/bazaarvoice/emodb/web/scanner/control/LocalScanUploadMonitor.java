package com.bazaarvoice.emodb.web.scanner.control;

import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.db.ScanRangeSplits;
import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.bazaarvoice.emodb.web.scanner.notifications.ScanCountListener;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanRangeStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatusDAO;
import com.bazaarvoice.emodb.web.scanner.writer.ScanWriter;
import com.bazaarvoice.emodb.web.scanner.writer.ScanWriterGenerator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Service which monitors active scans for when each scan range completes uploading.  It then analyzes the results and
 * performs one of the following actions:
 *
 * <ul>
 *     <li>
 *         If the upload was a failure it cancels the remainder of the scan.
 *     </li>
 *     <li>
 *         If the upload was a success and there are more scan ranges available for upload then signal that they
 *         are available.
 *     </li>
 *     <li>
 *         If the upload was a success and there are not more scan ranges available then complete the scan and clean
 *         up any temporary artifacts created by the scan, such as the distributed TableSet.
 *     </li>
 * </ul>
 */
public class LocalScanUploadMonitor extends AbstractService {

    // No scan can run for more than 1 day
    private static final Duration OVERRUN_SCAN_TIME = Duration.standardDays(1);

    private final Logger _log = LoggerFactory.getLogger(LocalScanUploadMonitor.class);

    private final ScanWorkflow _scanWorkflow;
    private final ScanStatusDAO _scanStatusDAO;
    private final ScanWriterGenerator _scanWriterGenerator;
    private final StashStateListener _stashStateListener;
    private final ScanCountListener _scanCountListener;
    private final DataTools _dataTools;
    private final Set<String> _activeScans = Sets.newHashSet();

    private ScheduledExecutorService _service;

    public LocalScanUploadMonitor(ScanWorkflow scanWorkflow, ScanStatusDAO scanStatusDAO,
                                  ScanWriterGenerator scanWriterGenerator,
                                  StashStateListener stashStateListener, ScanCountListener scanCountListener,
                                  DataTools dataTools) {
        _scanWorkflow = checkNotNull(scanWorkflow, "scanWorkflow");
        _scanStatusDAO = checkNotNull(scanStatusDAO, "scanStatusDAO");
        _scanWriterGenerator = checkNotNull(scanWriterGenerator, "scanWriterGenerator");
        _stashStateListener = checkNotNull(stashStateListener, "stashStateListener");
        _scanCountListener = checkNotNull(scanCountListener, "scanCountListener");
        _dataTools = checkNotNull(dataTools, "dataTools");
    }

    @VisibleForTesting
    public void setExecutorService(ScheduledExecutorService service) {
        _service = service;
    }

    @Override
    protected void doStart() {
        _log.info("Starting scan upload monitor service");

        if (_service == null) {
            _service = Executors.newScheduledThreadPool(1,
                    new ThreadFactoryBuilder().setNameFormat("scan-upload-monitor-%d").build());
        }

        // Perform the initialization asynchronously
        _service.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    // Initialize active scans with scans that were already active when this instance became leader
                    initializeAllActiveScans();

                    // Set the starting count for the scan count notifier
                    notifyActiveScanCountChanged();

                    // Start the loop for processing complete range scans
                    _service.schedule(_processCompleteRangeScansExecution, 1, TimeUnit.SECONDS);
                    
                    notifyStarted();
                } catch (Exception e) {
                    _log.error("Failed to start local scan upload monitor", e);
                    notifyFailed(e);
                }
            }
        });
    }

    @Override
    protected void doStop() {
        _log.info("Stopping scan upload monitor service");

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
        // will take over active scan count notifications.
        _activeScans.clear();
        notifyActiveScanCountChanged();

        notifyStopped();
    }

    private final Runnable _processCompleteRangeScansExecution = new Runnable() {
        @Override
        public void run() {
            try {
                processCompleteRangeScans();
            } catch (Exception e) {
                // This should never happen; all exceptions should already be caught in processCompleteRangeMigrations()
                _log.error("Unexpected exception caught processing complete range scans", e);
            }

            // Schedule the next check depending on whether any scans are active.
            long delaySeconds = _activeScans.isEmpty() ? 3 : 1;

            // Repeat execution after the previously computed delay
            _service.schedule(_processCompleteRangeScansExecution, delaySeconds, TimeUnit.SECONDS);
        }
    };

    protected void processCompleteRangeScans() {
        Multimap<String, ScanRangeComplete> completeRangeScansByScanId;

        try {
            completeRangeScansByScanId = Multimaps.index(
                    _scanWorkflow.claimCompleteScanRanges(Duration.standardMinutes(5)),
                    new Function<ScanRangeComplete, String>() {
                            @Override
                            public String apply(ScanRangeComplete completion) {
                                return completion.getScanId();
                            }
                    });
        } catch (Exception e) {
            _log.error("Failed to claim complete scan ranges", e);
            return;
        }

        for (Map.Entry<String, Collection<ScanRangeComplete>> entry : completeRangeScansByScanId.asMap().entrySet()) {
            String scanId = entry.getKey();
            Collection<ScanRangeComplete> completions = entry.getValue();

            try {
                refreshScan(scanId);
                _scanWorkflow.releaseCompleteScanRanges(completions);
            } catch (Exception e) {
                _log.error("Failed to process scan range complete: id={}", scanId, e);
            }
        }
    }

    @VisibleForTesting
    public void refreshScan(String id)
            throws IOException {
        ScanStatus status = _scanStatusDAO.getScanStatus(id);
        if (status == null) {
            _log.warn("Refresh scan called for unknown scan: {}", id);
            return;
        }

        if (status.isDone()) {
            if (status.isCanceled()) {
                scanCanceled(status);
            } else {
                completeScan(status);
            }
            return;
        }

        // Update the set of active scans
        if (_activeScans.add(id)) {
            // This is a new scan.  Notify that the number of active scans has changed.
            notifyActiveScanCountChanged();
            // Schedule a callback to cancel the scan if it goes overrun
            scheduleOverrunCheck(status);
        }

        // Before going any further ensure the stash table snapshot has been created and, if not, do so now.
        // This should only happen prior to the first scan range being processed.
        if (!status.isTableSnapshotCreated()) {
            _dataTools.createStashTokenRangeSnapshot(id, status.getOptions().getPlacements());
            _scanStatusDAO.setTableSnapshotCreated(id);
        }

        // Before evaluating available tasks check whether any completed tasks didn't scan their entire ranges
        // and require the addition of new tasks.
        status = resplitPartiallyCompleteTasks(status);

        Set<Integer> incompleteBatches = getIncompleteBatches(status);
        Multimap<Integer, ScanRangeStatus> queuedScansByConcurrencyId = getQueuedRangeScansByConcurrencyId(status);
        int maxConcurrency = status.getOptions().getMaxConcurrentSubRangeScans();
        Date now = new Date();

        for (ScanRangeStatus rangeStatus : getUnqueuedRangeScans(status)) {
            Optional<Integer> blockedByBatchId = rangeStatus.getBlockedByBatchId();
            Optional<Integer> concurrencyId = rangeStatus.getConcurrencyId();

            // Queue up the pending range scan if the following conditions are both met:
            // 1. It has no blocking batch or the blocking batch is complete
            // 2. The maximum concurrency permitted has not already been met
            if ((!blockedByBatchId.isPresent() || !incompleteBatches.contains(blockedByBatchId.get())) &&
                    (!concurrencyId.isPresent() || queuedScansByConcurrencyId.get(concurrencyId.get()).size() < maxConcurrency)) {
                int taskId = rangeStatus.getTaskId();
                String placement = rangeStatus.getPlacement();
                ScanRange range = rangeStatus.getScanRange();
                ScanRangeTask task = _scanWorkflow.addScanRangeTask(id, taskId, placement, range);
                _scanStatusDAO.setScanRangeTaskQueued(id, taskId, now);

                if (concurrencyId.isPresent()) {
                    // Mark that this range has been queued so this loop doesn't over-add range scans
                    queuedScansByConcurrencyId.put(concurrencyId.get(), rangeStatus);
                }

                _log.info("Queued scan range task: {}", task);
            }
        }
    }

    /**
     * Checks whether any completed tasks returned before scanning the entire range.  If so then the unscanned
     * ranges are resplit and new tasks are created from them.
     */
    private ScanStatus resplitPartiallyCompleteTasks(ScanStatus status) {
        boolean anyUpdated = false;
        int nextTaskId = -1;

        for (ScanRangeStatus complete : status.getCompleteScanRanges()) {
            if (complete.getResplitRange().isPresent()) {
                // This task only partially completed; there are still more data to scan.
                if (nextTaskId == -1) {
                    nextTaskId = getNextTaskId(status);
                }

                ScanRange resplitRange = complete.getResplitRange().get();

                // Resplit the un-scanned portion into new ranges
                List<ScanRange> subRanges = resplit(complete.getPlacement(), resplitRange, status.getOptions().getRangeScanSplitSize());
                // Create new tasks for each subrange that are immediately available for being queued.
                List<ScanRangeStatus> resplitStatuses = Lists.newArrayListWithCapacity(subRanges.size());
                for (ScanRange subRange : subRanges) {
                    resplitStatuses.add(
                            new ScanRangeStatus(nextTaskId++, complete.getPlacement(), subRange,
                                    complete.getBatchId(), complete.getBlockedByBatchId(), complete.getConcurrencyId()));
                }

                _scanStatusDAO.resplitScanRangeTask(status.getScanId(), complete.getTaskId(), resplitStatuses);

                anyUpdated = true;
            }
        }

        if (!anyUpdated) {
            return status;
        }

        // Slightly inefficient to reload but less risky than trying to keep the DAO and in-memory object in sync
        return _scanStatusDAO.getScanStatus(status.getScanId());
    }

    private int getNextTaskId(ScanStatus status) {
        Iterable<ScanRangeStatus> scanRangeStatuses = status.getAllScanRanges();
        if (Iterables.isEmpty(scanRangeStatuses)) {
            return 0;
        }

        return Ordering.natural().max(
                FluentIterable.from(scanRangeStatuses)
                        .transform(new Function<ScanRangeStatus, Integer>() {
                            @Override
                            public Integer apply(ScanRangeStatus scanRangeStatus) {
                                return scanRangeStatus.getTaskId();
                            }
                        }))
                + 1;
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
                .transform(new Function<ScanRangeStatus, Integer>() {
                    @Override
                    public Integer apply(ScanRangeStatus rangeStatus) {
                        return rangeStatus.getBatchId();
                    }
                })
                .toSet();
    }

    private Multimap<Integer, ScanRangeStatus> getQueuedRangeScansByConcurrencyId(ScanStatus status) {
        Iterable<ScanRangeStatus> incompleteRangeScans = Iterables.concat(status.getPendingScanRanges(), status.getActiveScanRanges());
        Multimap<Integer, ScanRangeStatus> queuedScansByConcurrencyId = ArrayListMultimap.create();
        for (ScanRangeStatus rangeStatus : incompleteRangeScans) {
            if (hasBeenQueued(rangeStatus) && rangeStatus.getConcurrencyId().isPresent()) {
                queuedScansByConcurrencyId.put(rangeStatus.getConcurrencyId().get(), rangeStatus);
            }
        }
        return queuedScansByConcurrencyId;
    }

    public List<ScanRangeStatus> getUnqueuedRangeScans(ScanStatus status) {
        // Analyze unqueued scan ranges by in scan-range order so sub-ranges will be queued deterministically
        return FluentIterable.from(status.getPendingScanRanges())
                .filter(new Predicate<ScanRangeStatus>() {
                    @Override
                    public boolean apply(ScanRangeStatus rangeStatus) {
                        return !hasBeenQueued(rangeStatus);
                    }
                })
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

    private void scanCanceled(ScanStatus status) {
        // Send notification that the scan has been canceled
        _stashStateListener.stashCanceled(status.asPluginStashMetadata(), new Date());
        cleanupScan(status.getScanId());
    }

    private void completeScan(ScanStatus status)
            throws IOException {
        String id = status.getScanId();
        if (status.getCompleteTime() != null) {
            _log.info("Scan already marked complete: {}", id);
            return;
        }

        _log.info("Scan complete: {}", id);

        try {
            // Mark the scan is complete
            Set<ScanDestination> destinations = status.getOptions().getDestinations();
            // Use -1 as the task ID since writing that the scan is complete is not associated with any scan range task.
            ScanWriter scanWriter = _scanWriterGenerator.createScanWriter(-1, destinations);
            scanWriter.writeScanComplete(id, _scanStatusDAO.getScanStatus(id).getStartTime());

            // Store the time the scan completed
            Date completeTime = new Date();
            _scanStatusDAO.setCompleteTime(id, completeTime);

            // Send notification that the scan has completed.
            status.setCompleteTime(completeTime);
            _stashStateListener.stashCompleted(status.asPluginStashMetadata(), status.getCompleteTime());
        } finally {
            cleanupScan(id);
        }
    }

    private void cleanupScan(String id) {
        // Remove this scan from the active set
        if (_activeScans.remove(id)) {
            notifyActiveScanCountChanged();
        }

        try {
            // Remove the table snapshots set for this scan
            _dataTools.clearStashTokenRangeSnapshot(id);
        } catch (Exception e) {
            _log.error("Failed to clean up table set for scan {}", id, e);
        }
    }

    private void initializeAllActiveScans() {
        try {
            Iterator<ScanStatus> scanStatuses = _scanStatusDAO.list(null, Long.MAX_VALUE);
            while (scanStatuses.hasNext()) {
                ScanStatus status = scanStatuses.next();
                if (!status.isDone()) {
                    // Record that the scan is active
                    _activeScans.add(status.getScanId());
                    // Schedule a callback to cancel the scan if it goes overrun
                    scheduleOverrunCheck(status);
                }
            }
        } catch (Exception e) {
            // This is bad but not critical; if there are any active scans they will likely check in with a completed
            // scan range within several minutes anyway.
            _log.warn("Failed to initialize active scan count", e);
        }
    }

    private void scheduleOverrunCheck(ScanStatus status) {
        final String scanId = status.getScanId();

        DateTime now = DateTime.now();
        DateTime overrunTime = new DateTime(status.getStartTime()).plus(OVERRUN_SCAN_TIME);

        long delay = 0;
        if (now.isBefore(overrunTime)) {
            delay = new Duration(now, overrunTime).getMillis();
        }

        _service.schedule(
                new Runnable() {
                    @Override
                    public void run() {
                        ScanStatus status = _scanStatusDAO.getScanStatus(scanId);
                        if (!status.isDone()) {
                            _log.warn("Overrun scan detected, canceling scan: {}", scanId);
                            _scanStatusDAO.setCanceled(scanId);
                            _scanWorkflow.scanStatusUpdated(scanId);

                        }
                    }
                },
                delay, TimeUnit.MILLISECONDS);
    }

    private void notifyActiveScanCountChanged() {
        _scanCountListener.activeScanCountChanged(_activeScans.size());
    }
}
