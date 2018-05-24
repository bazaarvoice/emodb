package com.bazaarvoice.emodb.web.scanner.scheduling;

import com.bazaarvoice.emodb.sor.api.InvalidStashRequestException;
import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.bazaarvoice.emodb.web.scanner.scanstatus.InMemoryStashRequestDAO;
import com.bazaarvoice.emodb.web.scanner.scanstatus.StashRequest;
import com.bazaarvoice.emodb.web.scanner.scanstatus.StashRequestDAO;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class StashRequestManagerTest {

    private StashRequestDAO _stashRequestDAO;
    private Clock _clock;
    private StashRequestManager _stashRequestManager;

    @BeforeMethod
    public void setUp() {
        _stashRequestDAO = new InMemoryStashRequestDAO();
        _clock = mock(Clock.class);

        // Create two daily scans, one which runs on request and one which doesn'
        List<ScheduledDailyScanUpload> scanUploads = ImmutableList.of(
                new ScheduledDailyScanUpload("always", "00:00Z", DateTimeFormatter.ofPattern("'always'-yyyy-MM-dd-HH-mm-ss"),
                        ScanDestination.discard(), DateTimeFormatter.ofPattern("'dest'-yyyy-MM-dd-HH-mm-ss"),
                        ImmutableList.of("ugc_global:ugc"), 4, true, false, 1000000, Duration.ofMinutes(10)),
                new ScheduledDailyScanUpload("byrequest", "12:00Z", DateTimeFormatter.ofPattern("'byrequest'-yyyy-MM-dd-HH-mm-ss"),
                        ScanDestination.discard(), DateTimeFormatter.ofPattern("'dest'-yyyy-MM-dd-HH-mm-ss"),
                        ImmutableList.of("ugc_global:ugc"), 4, true, true, 1000000, Duration.ofMinutes(10))
        );

        _stashRequestManager = new StashRequestManager(_stashRequestDAO, scanUploads, _clock);
    }

    @Test (expectedExceptions = InvalidStashRequestException.class)
    public void testCreateRequestNonExistentStash() {
        Instant now = Instant.parse("2017-11-20T23:30:00.000Z");
        setClockTo(now);
        _stashRequestManager.requestStashOnOrAfter("nosuchid", now, "requester1");
    }

    @Test (expectedExceptions = InvalidStashRequestException.class)
    public void testCreateRequestForNoRequestRequiredStash() {
        Instant now = Instant.parse("2017-11-20T23:30:00.000Z");
        setClockTo(now);
        _stashRequestManager.requestStashOnOrAfter("always", now, "requester1");
    }

    @Test
    public void testCreateRequestFutureStash() {
        Instant now = Instant.parse("2017-11-20T11:30:00.000Z");
        setClockTo(now);
        _stashRequestManager.requestStashOnOrAfter("byrequest", now, "requester1");

        assertEquals(_stashRequestDAO.getRequestsForStash("byrequest-2017-11-20-12-00-00"), ImmutableSet.of(new StashRequest("requester1", new Date(now.toEpochMilli()))));
    }

    @Test
    public void testCreateRequestAtTimeOfStash() {
        Instant now = Instant.parse("2017-11-20T12:00:00.000Z");
        setClockTo(now);
        _stashRequestManager.requestStashOnOrAfter("byrequest", now, "requester1");

        assertEquals(_stashRequestDAO.getRequestsForStash("byrequest-2017-11-20-12-00-00"), ImmutableSet.of(new StashRequest("requester1", new Date(now.toEpochMilli()))));
    }

    @Test (expectedExceptions = InvalidStashRequestException.class)
    public void testCreateRequestTooFarInPast() {
        Instant now = Instant.parse("2017-11-20T11:30:00.000Z");
        setClockTo(now);
        _stashRequestManager.requestStashOnOrAfter("byrequest", now.minus(Duration.ofDays(1)), "requester1");
    }

    @Test (expectedExceptions = InvalidStashRequestException.class)
    public void testUndoCreateRequestForNonExistentStash() {
        Instant now = Instant.parse("2017-11-20T23:30:00.000Z");
        setClockTo(now);
        _stashRequestManager.undoRequestForStashOnOrAfter("nosuchid", now, "requester1");
    }

    @Test (expectedExceptions = InvalidStashRequestException.class)
    public void testUndoCreateRequestForNoRequestRequiredStash() {
        Instant now = Instant.parse("2017-11-20T23:30:00.000Z");
        setClockTo(now);
        _stashRequestManager.requestStashOnOrAfter("always", now, "requester1");
    }

    @Test
    public void testUndoCreateRequestFutureStash() {
        Instant now = Instant.parse("2017-11-20T11:30:00.000Z");
        _stashRequestDAO.requestStash("byrequest-2017-11-20-12-00-00", new StashRequest("requester1", new Date(now.minus(Duration.ofMinutes(1)).toEpochMilli())));
        setClockTo(now);
        _stashRequestManager.undoRequestForStashOnOrAfter("byrequest", now, "requester1");

        assertEquals(_stashRequestDAO.getRequestsForStash("byrequest-2017-11-20-12-00-00"), ImmutableSet.of());
    }

    @Test (expectedExceptions = InvalidStashRequestException.class)
    public void testUndoCreateRequestTooFarInPast() {
        Instant now = Instant.parse("2017-11-20T11:30:00.000Z");
        setClockTo(now);
        _stashRequestManager.undoRequestForStashOnOrAfter("byrequest", now.minus(Duration.ofDays(1)), "requester1");
    }

    @Test
    public void testGetRequestsForNonExistentStash() {
        Instant now = Instant.parse("2017-11-20T23:30:00.000Z");
        setClockTo(now);
        Set<StashRequest> requests = _stashRequestManager.getRequestsForStash("nosuchid", now);
        assertEquals(requests, ImmutableSet.of());
    }

    @Test
    public void testGetRequestsForNoRequestRequiredStash() {
        Instant now = Instant.parse("2017-11-20T23:30:00.000Z");
        setClockTo(now);
        Set<StashRequest> requests = _stashRequestManager.getRequestsForStash("always", now);
        assertEquals(requests, ImmutableSet.of());
    }

    @Test
    public void testGetRequestsFutureStash() {
        Instant now = Instant.parse("2017-11-20T11:30:00.000Z");
        StashRequest request = new StashRequest("requester1", new Date(now.minus(Duration.ofMinutes(1)).toEpochMilli()));
        _stashRequestDAO.requestStash("byrequest-2017-11-20-12-00-00", request);
        setClockTo(now);
        Set<StashRequest> requests = _stashRequestManager.getRequestsForStash("byrequest", now);

        assertEquals(requests, ImmutableSet.of(request));
    }

    @Test
    public void testGetRequestsAtTimeOfStash() {
        Instant now = Instant.parse("2017-11-20T12:00:00.000Z");
        StashRequest request = new StashRequest("requester1", new Date(now.minus(Duration.ofMinutes(1)).toEpochMilli()));
        _stashRequestDAO.requestStash("byrequest-2017-11-20-12-00-00", request);
        setClockTo(now);
        Set<StashRequest> requests = _stashRequestManager.getRequestsForStash("byrequest", now);

        assertEquals(requests, ImmutableSet.of(request));
    }
    
    private void setClockTo(Instant when) {
        when(_clock.instant()).thenReturn(when);
        when(_clock.millis()).thenReturn(when.toEpochMilli());
    }
}
