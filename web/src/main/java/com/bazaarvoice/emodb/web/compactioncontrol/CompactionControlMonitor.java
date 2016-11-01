package com.bazaarvoice.emodb.web.compactioncontrol;

import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A service that checks the stash times and deletes the expired entries.
 */
public class CompactionControlMonitor extends AbstractScheduledService {
    private static final Logger _log = LoggerFactory.getLogger(CompactionControlMonitor.class);

    @VisibleForTesting
    protected static final Duration POLL_INTERVAL = Duration.standardHours(1);

    @VisibleForTesting
    protected static final Long CURRENT_TIME = System.currentTimeMillis();

    private final CompactionControlSource _compactionControlSource;
    private final DataCenters _dataCenters;

    public CompactionControlMonitor(CompactionControlSource compactionControlSource, DataCenters dataCenters) {
        _compactionControlSource = checkNotNull(compactionControlSource, "compactionControlSource");
        _dataCenters = checkNotNull(dataCenters, "dataCenters");
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
            deleteExpiredStashTimes();
        } catch (Throwable t) {
            _log.error("Unexpected exception.", t);
        }
        ;
    }

    private void deleteExpiredStashTimes() {
        try {
            _log.debug("Checking for expired stash times at {}", CURRENT_TIME);
            Set<String> expiredIds = Maps.filterValues(_compactionControlSource.getAllStashTimes(), value -> value.getExpiredTimestamp() > CURRENT_TIME).keySet();
            for (String id : expiredIds) {
                // If we are deleting the entries here, then there could be a problem which we may want to know. So setting it as an ERROR.
                _log.error("Deleting the stash time entry for id: {}", id);
                _compactionControlSource.deleteStashTime(id, _dataCenters.getSelf().getName());
            }
        } catch (Exception e) {
            _log.error("Unexpected exception deleting the expired stash times", e);
        }
    }
}
