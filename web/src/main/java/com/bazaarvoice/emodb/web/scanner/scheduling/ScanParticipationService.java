package com.bazaarvoice.emodb.web.scanner.scheduling;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.time.Clock;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Service which announces that this server is ready to participate at the beginning of each configured scheduled scan.
 * This allows for alerting if there are not enough participants for the scan to finish in a timely manner.
 */
public class ScanParticipationService implements Managed {

    private final List<ScheduledDailyScanUpload> _scheduledScans;
    private final Runnable _notificationRunnable;
    private final List<Future<?>> _participationFutures = Lists.newArrayList();
    private ScheduledExecutorService _service;
    private boolean _shutdownServiceOnStop;
    private final Clock _clock;

    @Inject
    public ScanParticipationService(List<ScheduledDailyScanUpload> scheduledScans, final StashStateListener stashStateListener,
                                    LifeCycleRegistry lifeCycleRegistry, Clock clock) {
        _scheduledScans = scheduledScans;
        _clock = clock;

        // Create a runnable which announces scan participation when called
        _notificationRunnable = new Runnable() {
            @Override
            public void run() {
                stashStateListener.announceStashParticipation();
            }
        };

        lifeCycleRegistry.manage(this);
    }

    @VisibleForTesting
    public void setScheduledExecutorService(ScheduledExecutorService service) {
        _service = service;
    }

    @Override
    public void start() throws Exception {
        if (_service == null) {
            _service = Executors.newScheduledThreadPool(
                    1, new ThreadFactoryBuilder().setNameFormat("scan-upload-participation-%d").build());
            _shutdownServiceOnStop = true;
        }

        for (ScheduledDailyScanUpload scheduledScan : _scheduledScans) {
            Future<?> participationFuture = scheduleParticipationNotification(scheduledScan);
            _participationFutures.add(participationFuture);
        }
    }

    private Future<?> scheduleParticipationNotification(ScheduledDailyScanUpload scheduledScan) {
        // Get the next execution time for the scheduled scan
        DateTime now = new DateTime(_clock.millis());
        DateTime nextExecutionTime = scheduledScan.getNextExecutionTimeAfter(now);

        // Schedule to run once daily starting at the next execution time
        return _service.scheduleAtFixedRate(
                _notificationRunnable,
                new Duration(now, nextExecutionTime).getMillis(),
                Duration.standardDays(1).getMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() throws Exception {
        for (Future<?> participationFuture : _participationFutures) {
            participationFuture.cancel(false);
        }
        if (_shutdownServiceOnStop) {
            _service.shutdownNow();
        }
    }
}
