package com.bazaarvoice.emodb.web.scanner.scheduling;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.ScanUploader;
import com.bazaarvoice.emodb.web.scanner.ScannerZooKeeper;
import com.bazaarvoice.emodb.web.scanner.notifications.ScanCountListener;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Service ensures that each scheduled scan is started at the configured time daily.  Leadership election is used to
 * ensure it is started by exactly one server.
 */
public class ScanUploadSchedulingService extends LeaderService {

    private static final String SERVICE_NAME = "scan-upload-scheduler";
    private static final String LEADER_DIR = "/leader/scheduler";

    private static final Duration SCAN_PENDING_PERIOD = Duration.ofMinutes(45);

    @Inject
    public ScanUploadSchedulingService(@ScannerZooKeeper CuratorFramework curator, @SelfHostAndPort HostAndPort selfHostAndPort,
                                       final ScanUploader scanUploader, final List<ScheduledDailyScanUpload> scheduledScans,
                                       final ScanCountListener scanCountListener, final StashRequestManager stashRequestManager,
                                       LifeCycleRegistry lifecycle, LeaderServiceTask leaderServiceTask,
                                       final MetricRegistry metricRegistry,
                                       final Clock clock) {
        super(curator, LEADER_DIR, selfHostAndPort.toString(), SERVICE_NAME, 1, TimeUnit.MINUTES,
                new Supplier<Service>() {
                    @Override
                    public Service get() {
                        return new DelegateSchedulingService(scanUploader, stashRequestManager, scheduledScans, scanCountListener, clock);
                    }
                });

        ServiceFailureListener.listenTo(this, metricRegistry);
        leaderServiceTask.register(SERVICE_NAME, this);
        lifecycle.manage(new ManagedGuavaService(this));
    }

    public static class DelegateSchedulingService extends AbstractService {

        private final Logger _log = LoggerFactory.getLogger(ScanUploadSchedulingService.class);

        private final ScanUploader _scanUploader;
        private final StashRequestManager _stashRequestManager;
        private final List<ScheduledDailyScanUpload> _scheduledScans;
        private final ScanCountListener _scanCountListener;
        private final Set<ScheduledDailyScanUpload> _pendingScans = Sets.newHashSet();
        private final Clock _clock;
        private ScheduledExecutorService _service;

        public DelegateSchedulingService(ScanUploader scanUploader, StashRequestManager stashRequestManager,
                                         List<ScheduledDailyScanUpload> scheduledScans,
                                         ScanCountListener scanCountListener, Clock clock) {
            _scanUploader = scanUploader;
            _stashRequestManager = stashRequestManager;
            _scheduledScans = scheduledScans;
            _scanCountListener = scanCountListener;
            _clock = clock;
        }

        @VisibleForTesting
        void setExecutorService(ScheduledExecutorService service) {
            _service = service;
        }

        @Override
        protected void doStart() {
            _log.info("Starting scan upload scheduling service");

            if (_scheduledScans.isEmpty()) {
                _log.info("No scan uploads scheduled; service taking no action");
            } else {
                if (_service == null) {
                    _service = Executors.newScheduledThreadPool(1,
                            new ThreadFactoryBuilder().setNameFormat("scheduled-scan-uploads-%d").build());
                }

                // Asynchronously start all scheduled scans
                _service.submit(new Runnable() {
                    @Override
                    public void run() {
                        initializeScans();
                    }
                });
            }

            notifyStarted();
        }

        @VisibleForTesting
        void initializeScans() {
            // Schedule each scan
            for (ScheduledDailyScanUpload scheduledScan : _scheduledScans) {
                scheduleScan(scheduledScan);
            }

            // Send notification if there are any scans pending after scheduling all scans
            notifyPendingScanCountChanged();

            // There is a brief window when ZooKeeper leadership changes hands when no server is the leader.
            // When we acquire leadership look back to ensure that scans which may have been scheduled
            // during that window are not missed.
            checkForMissedScans();
        }

        @Override
        protected void doStop() {
            _log.info("Stopping scan upload scheduling service");

            if (_service != null) {
                _service.shutdownNow();
                _service = null;
            }

            // Send a final notification that the pending count is zero; whoever picks up leadership going forward
            // will take over pending scan count notifications.
            _pendingScans.clear();
            notifyPendingScanCountChanged();

            notifyStopped();
        }

        /**
         * Schedule a daily scan and upload to run once daily.
         */
        private void scheduleScan(final ScheduledDailyScanUpload scanUpload) {
            // Schedule the first iteration for this scan

            Instant now = _clock.instant();
            final Instant nextExecTime = scanUpload.getNextExecutionTimeAfter(now);
            scheduleNextScanExecution(scanUpload, now, nextExecTime);

            // Schedule the pending scan count to increment 45 minutes before the scan begins.

            Instant pendingExecTime = nextExecTime.minus(SCAN_PENDING_PERIOD);
            if (pendingExecTime.isBefore(now)) {
                // We're already within the pending exec time.  Mark that the scan is pending and schedule the
                // first iteration for the next day.
                maybeAddPendingScan(scanUpload, nextExecTime);
                pendingExecTime = pendingExecTime.plus(Duration.ofDays(1));
            }

            _service.scheduleAtFixedRate(
                    () -> maybeAddPendingScan(scanUpload, scanUpload.getNextExecutionTimeAfter(_clock.instant())),
                    Duration.between(now, pendingExecTime).toMillis(),
                    Duration.ofDays(1).toMillis(),
                    TimeUnit.MILLISECONDS);
        }

        private void maybeAddPendingScan(final ScheduledDailyScanUpload scanUpload, final Instant nextExecutionTime) {
            if (scanUpload.isRequestRequired() && _stashRequestManager.getRequestsForStash(scanUpload.getId(), nextExecutionTime).isEmpty()) {
                // This scan runs only by request and there are currently no requests for this scan.  However, that
                // could change between now and the next execution time.  Schedule to re-check in 30 seconds.
                if (_clock.instant().isBefore(nextExecutionTime.minus(Duration.ofMinutes(1)))) {
                    _service.schedule(() -> maybeAddPendingScan(scanUpload, nextExecutionTime), 30, TimeUnit.SECONDS);
                }
                return;
            }

            // Last chance to make sure the scan is still pending
            if (_clock.instant().isBefore(nextExecutionTime) && _pendingScans.add(scanUpload)) {
                notifyPendingScanCountChanged();
            }
        }

        /**
         * Schedules the scan and upload to execute at the given time, then daily at the same time afterward indefinitely.
         */
        private void scheduleNextScanExecution(final ScheduledDailyScanUpload scanUpload, Instant now, final Instant nextExecTime) {
            Duration delay = Duration.between(now, nextExecTime);

            // We deliberately chain scheduled scans instead of scheduling at a fixed rate to allow for
            // each iteration to explicitly contain the expected execution time.
            _service.schedule(
                    new Runnable() {
                        @Override
                        public void run() {
                            try {
                                startScheduledScan(scanUpload, nextExecTime);
                            } catch (Exception e) {
                                _log.error("Failed to start scheduled daily scan upload", e);
                            }

                            // Remove this scan from the pending set
                            if (_pendingScans.remove(scanUpload)) {
                                notifyPendingScanCountChanged();
                            }

                            // Schedule the next run
                            scheduleNextScanExecution(scanUpload, _clock.instant(), nextExecTime.plus(Duration.ofDays(1)));
                        }
                    },
                    delay.toMillis(), TimeUnit.MILLISECONDS);

            _log.info("Scan and upload to {} scheduled to execute at {} ({} from now)",
                    scanUpload.getRootDestination(), nextExecTime, delay);
        }

        /**
         * Starts a scheduled scan and upload which had been scheduled to start at the given time.  This method will
         * throw an exception if the current time isn't reasonably close to the scheduled time or if the scan for
         * the scheduled time has already started.
         */
        @VisibleForTesting
        synchronized ScanStatus startScheduledScan(ScheduledDailyScanUpload scheduledScan, Instant scheduledTime)
                throws RepeatScanException, ScanExecutionTimeException {
            // Verify that the scan either doesn't require requests or has at least one request
            if (scheduledScan.isRequestRequired() && _stashRequestManager.getRequestsForStash(scheduledScan.getId(), scheduledTime).isEmpty()) {
                _log.info("Scan {} did not receive any requests and will not be executed for {}",
                        scheduledScan.getId(), scheduledTime);
                return null;
            }

            // Name the scan ID and directory for when the scan was scheduled
            String scanId = scheduledScan.getScanIdFormat().format(scheduledTime);
            String directory = scheduledScan.getDirectoryFormat().format(scheduledTime);

            ScanDestination destination = scheduledScan.getRootDestination().getDestinationWithSubpath(directory);

            // Verify this scan hasn't already been started
            if (_scanUploader.getStatus(scanId) != null) {
                throw new RepeatScanException("Scan has already been started: " + scanId);
            }

            // Allow the scan to start up to 30 seconds early or 10 minutes late
            Instant now = _clock.instant();
            Range<Instant> acceptableInterval = Range.closed(scheduledTime.minusSeconds(30), scheduledTime.plus(Duration.ofMinutes(10)));
            if (!acceptableInterval.contains(now)) {
                throw new ScanExecutionTimeException(String.format(
                        "Scheduled scan to %s is not running at the expected time: expected = %s, actual = %s",
                        destination, scheduledTime, now));
            }

            ScanOptions scanOptions = new ScanOptions(scheduledScan.getPlacements())
                    .addDestination(destination)
                    .setMaxConcurrentSubRangeScans(scheduledScan.getMaxRangeConcurrency())
                    .setScanByAZ(scheduledScan.isScanByAZ())
                    .setRangeScanSplitSize(scheduledScan.getMaxRangeScanSplitSize())
                    .setMaxRangeScanTime(scheduledScan.getMaxRangeScanTime());

            _log.info("Starting scheduled scan and upload to {} for time {}", destination, scheduledTime);

            return _scanUploader.scanAndUpload(scanId, scanOptions).start();
        }

        /**
         * Checks all scheduled scans and tries to start any which should have stared within the last 10 minutes.
         * This ensures that ZooKeeper and/or leadership outages of less than 10 minutes will not cause
         * a scheduled scan to be completely missed, just possibly delayed.
         */
        private void checkForMissedScans() {
            Instant now = _clock.instant();
            for (ScheduledDailyScanUpload scheduledScan : _scheduledScans) {
                // Try to start the scan if it's within 10 minutes after the scan should have started
                Instant startCheckInterval = now.minus(Duration.ofMinutes(10));
                Range<Instant> missedInterval = Range.closed(startCheckInterval, now);
                Instant scheduledTime = scheduledScan.getNextExecutionTimeAfter(startCheckInterval);

                if (missedInterval.contains(scheduledTime)) {
                    _log.info("Attempting to start potentially missed scan for time {}", scheduledTime);

                    try {
                        startScheduledScan(scheduledScan, scheduledTime);
                    } catch (RepeatScanException e) {
                        // OK, the scan wasn't missed
                        _log.info("Scan was not missed for time {}, no action taken", scheduledTime);
                    } catch (ScanExecutionTimeException e) {
                        // Must have just passed beyond the accepted execution time
                        _log.info("Too much time has elapsed since {}, no action taken", scheduledTime);
                    }
                }
            }
        }

        private void notifyPendingScanCountChanged() {
            _scanCountListener.pendingScanCountChanged(_pendingScans.size());
        }
    }

    /**
     * Internal exception thrown when a scan is being executed too far after its scheduled time.
     */
    static class ScanExecutionTimeException extends Exception {
        private ScanExecutionTimeException(String message) {
            super(message);
        }
    }

    /**
     * Internal exception thrown when a scan is being scheduled more than once.
     */
    static class RepeatScanException extends Exception {
        private RepeatScanException(String message) {
            super(message);
        }
    }
}
