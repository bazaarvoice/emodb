package com.bazaarvoice.emodb.web.scanner.scheduling;

import com.bazaarvoice.emodb.sor.api.InvalidStashRequestException;
import com.bazaarvoice.emodb.web.scanner.scanstatus.StashRequest;
import com.bazaarvoice.emodb.web.scanner.scanstatus.StashRequestDAO;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.time.Clock;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manager for maintaining the state of stash requests.  Mostly acts as an entry point to a {@link StashRequestDAO} with
 * additional business logic and state validation.
 */
public class StashRequestManager {

    private final StashRequestDAO _stashRequestDAO;
    private final Map<String, ScheduledDailyScanUpload> _scheduledScans;
    private final Clock _clock;

    @Inject
    public StashRequestManager(StashRequestDAO stashRequestDAO, List<ScheduledDailyScanUpload> scheduledScans, Clock clock) {
        _stashRequestDAO = checkNotNull(stashRequestDAO, "stashRequestDAO");
        _scheduledScans = Maps.uniqueIndex(
                checkNotNull(scheduledScans, "scheduledScans"),
                ScheduledDailyScanUpload::getId);
        _clock = checkNotNull(clock, "clock");
    }

    public void requestStashOnOrAfter(String id, @Nullable DateTime time, String requestedBy) {
        checkNotNull(id, "id");
        checkNotNull(requestedBy, "requestedBy");

        ScheduledDailyScanUpload scheduledScan = getAndValidateScan(id);
        DateTime now = new DateTime(_clock.millis());
        DateTime nextExecutionTime = scheduledScan.getNextExecutionTimeAfter(requestedTimeOrNow(time));
        if (nextExecutionTime.isBefore(now)) {
            throw new InvalidStashRequestException("Requested stash is in the past");
        }

        String scanId = scheduledScan.getScanIdFormat().print(nextExecutionTime);
        _stashRequestDAO.requestStash(scanId, new StashRequest(requestedBy, new Date(_clock.millis())));
    }

    public void undoRequestForStashOnOrAfter(String id, @Nullable DateTime time, String requestedBy) {
        checkNotNull(id, "id");
        checkNotNull(requestedBy, "requestedBy");

        ScheduledDailyScanUpload scheduledScan = getAndValidateScan(id);
        DateTime now = new DateTime(_clock.millis());
        DateTime nextExecutionTime = scheduledScan.getNextExecutionTimeAfter(requestedTimeOrNow(time));
        if (nextExecutionTime.isBefore(now)) {
            throw new InvalidStashRequestException("Requested stash is in the past");
        }

        String scanId = scheduledScan.getScanIdFormat().print(nextExecutionTime);
        _stashRequestDAO.undoRequestStash(scanId, new StashRequest(requestedBy, new Date(_clock.millis())));
    }

    public Set<StashRequest> getRequestsForStash(String id, @Nullable DateTime time) {
        checkNotNull(id, "id");

        ScheduledDailyScanUpload scheduledScan = _scheduledScans.get(id);
        if (scheduledScan == null || !scheduledScan.isRequestRequired()) {
            return ImmutableSet.of();
        }

        DateTime nextExecutionTime = scheduledScan.getNextExecutionTimeAfter(requestedTimeOrNow(time));
        String scanId = scheduledScan.getScanIdFormat().print(nextExecutionTime);
        return _stashRequestDAO.getRequestsForStash(scanId);
    }

    private ScheduledDailyScanUpload getAndValidateScan(String id) {
        ScheduledDailyScanUpload scheduledScan = _scheduledScans.get(id);
        if (scheduledScan == null) {
            throw new InvalidStashRequestException("No stash found with ID: " + id);
        }
        if (!scheduledScan.isRequestRequired()) {
            throw new InvalidStashRequestException("Stash not configured to accept requests: " + id);
        }
        return scheduledScan;
    }
    
    private DateTime requestedTimeOrNow(@Nullable DateTime time) {
        return time != null ? time :  new DateTime(_clock.millis());
    }
}
