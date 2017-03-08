package com.bazaarvoice.emodb.web.compactioncontrol;

import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.google.common.annotations.VisibleForTesting;
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
 * A service that checks the stash times and deletes the expired entries.
 */
public class CompactionControlMonitor extends AbstractScheduledService {
    private static final Logger _log = LoggerFactory.getLogger(CompactionControlMonitor.class);

    @VisibleForTesting
    protected static final Duration POLL_INTERVAL = Duration.standardHours(1);

    private final CompactionControlSource _compactionControlSource;
    private final Clock _clock;

    public CompactionControlMonitor(CompactionControlSource compactionControlSource, Clock clock) {
        _compactionControlSource = checkNotNull(compactionControlSource, "compactionControlSource");
        _clock = checkNotNull(clock, "clock");
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
            deleteExpiredStashTimes(_clock.millis());
        } catch (Throwable t) {
            _log.error("Unexpected exception.", t);
        }
        ;
    }

    @VisibleForTesting
    protected void deleteExpiredStashTimes(long currentTime) {
        try {
            _log.debug("Checking for expired stash times at {}", currentTime);
            Map<String, StashRunTimeInfo> expiredStashTimes = _compactionControlSource.getAllStashTimes().entrySet().stream()
                    .filter(entry -> entry.getValue().getExpiredTimestamp() < currentTime).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
