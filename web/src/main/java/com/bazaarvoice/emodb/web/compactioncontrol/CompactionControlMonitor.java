package com.bazaarvoice.emodb.web.compactioncontrol;

import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A service that checks the stash times, updates the metrics and deletes the expired entries.
 */
public class CompactionControlMonitor extends AbstractScheduledService {
    private static final Logger _log = LoggerFactory.getLogger(CompactionControlMonitor.class);

    @VisibleForTesting
    protected static final Duration POLL_INTERVAL = Duration.standardMinutes(5);

    private final CompactionControlSource _compactionControlSource;
    private final Clock _clock;
    private volatile long _lag;

    public CompactionControlMonitor(CompactionControlSource compactionControlSource, Clock clock, MetricRegistry metricRegistry) {
        _compactionControlSource = checkNotNull(compactionControlSource, "compactionControlSource");
        _clock = checkNotNull(clock, "clock");
        checkNotNull(metricRegistry, "metricRegistry").register(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "compaction-control-time-lag"), (Gauge<Long>) () -> _lag);
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, POLL_INTERVAL.getMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void shutDown() {
    }

    @Override
    protected void runOneIteration() {
        try {
            updateMetrics(_clock.millis());
            deleteExpiredStashTimes(_clock.millis());
        } catch (Throwable t) {
            _log.error("Unexpected exception.", t);
        }
    }

    private void updateMetrics(long currentTimeInMillis) {
        Map<String, StashRunTimeInfo> stashTimeInfoMap = _compactionControlSource.getAllStashTimes();
        if (stashTimeInfoMap.size() > 0) {
            long oldestCompactionControlTime = stashTimeInfoMap.values().stream()
                    .map(StashRunTimeInfo::getTimestamp)
                    .min(Ordering.natural())
                    .orElse(currentTimeInMillis);
            _lag = currentTimeInMillis - oldestCompactionControlTime;
        } else {
            _lag = 0L;
        }
    }

    @VisibleForTesting
    protected void deleteExpiredStashTimes(long currentTimeInMillis) {
        try {
            _log.debug("Checking for expired stash times at {}", currentTimeInMillis);
            Map<String, StashRunTimeInfo> expiredStashTimes = _compactionControlSource.getAllStashTimes().entrySet().stream()
                    .filter(entry -> entry.getValue().getExpiredTimestamp() < currentTimeInMillis).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            for (Map.Entry<String, StashRunTimeInfo> expiredStashTimeInfo : expiredStashTimes.entrySet()) {
                // If we are deleting the entries here, then there could be a problem which we may want to know. So setting it as a warn.
                _log.warn("Deleting the stash time entry for id: {}", expiredStashTimeInfo.getKey());
                _compactionControlSource.deleteStashTime(expiredStashTimeInfo.getKey(), expiredStashTimeInfo.getValue().getDataCenter());
            }
        } catch (Exception e) {
            _log.error("Unexpected exception deleting the expired stash times", e);
        }
    }
}
