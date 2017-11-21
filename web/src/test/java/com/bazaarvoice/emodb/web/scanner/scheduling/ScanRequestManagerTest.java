package com.bazaarvoice.emodb.web.scanner.scheduling;

import com.bazaarvoice.emodb.sor.api.InvalidStashRequestException;
import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.bazaarvoice.emodb.web.scanner.scanstatus.InMemoryScanRequestDAO;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanRequest;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanRequestDAO;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Clock;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class ScanRequestManagerTest {

    private ScanRequestDAO _scanRequestDAO;
    private Clock _clock;
    private ScanRequestManager _scanRequestManager;

    @BeforeMethod
    public void setUp() {
        _scanRequestDAO = new InMemoryScanRequestDAO();
        _clock = mock(Clock.class);

        // Create two daily scans, one which runs on request and one which doesn'
        List<ScheduledDailyScanUpload> scanUploads = ImmutableList.of(
                new ScheduledDailyScanUpload("always", "00:00Z", DateTimeFormat.forPattern("'always'-yyyy-MM-dd-HH-mm-ss"),
                        ScanDestination.discard(), DateTimeFormat.forPattern("'dest'-yyyy-MM-dd-HH-mm-ss"),
                        ImmutableList.of("ugc_global:ugc"), 4, true, false),
                new ScheduledDailyScanUpload("byrequest", "12:00Z", DateTimeFormat.forPattern("'byrequest'-yyyy-MM-dd-HH-mm-ss"),
                        ScanDestination.discard(), DateTimeFormat.forPattern("'dest'-yyyy-MM-dd-HH-mm-ss"),
                        ImmutableList.of("ugc_global:ugc"), 4, true, true)
        );

        _scanRequestManager = new ScanRequestManager(_scanRequestDAO, scanUploads, _clock);
    }

    @Test (expectedExceptions = InvalidStashRequestException.class)
    public void testCreateRequestNonExistentStash() {
        DateTime now = ISODateTimeFormat.dateTime().parseDateTime("2017-11-20T23:30:00.000Z");
        when(_clock.millis()).thenReturn(now.getMillis());
        _scanRequestManager.requestScanOnOrAfter("nosuchid", now, "requester1");
    }

    @Test (expectedExceptions = InvalidStashRequestException.class)
    public void testCreateRequestForNoRequestRequiredStash() {
        DateTime now = ISODateTimeFormat.dateTime().parseDateTime("2017-11-20T23:30:00.000Z");
        when(_clock.millis()).thenReturn(now.getMillis());
        _scanRequestManager.requestScanOnOrAfter("always", now, "requester1");
    }

    @Test
    public void testCreateRequestFutureStash() {
        DateTime now = ISODateTimeFormat.dateTime().parseDateTime("2017-11-20T11:30:00.000Z");
        when(_clock.millis()).thenReturn(now.getMillis());
        _scanRequestManager.requestScanOnOrAfter("byrequest", now, "requester1");

        assertEquals(_scanRequestDAO.getRequestsForScan("byrequest-2017-11-20-12-00-00"), ImmutableSet.of(new ScanRequest("requester1", new Date(now.getMillis()))));
    }

    @Test
    public void testCreateRequestAtTimeOfStash() {
        DateTime now = ISODateTimeFormat.dateTime().parseDateTime("2017-11-20T12:00:00.000Z");
        when(_clock.millis()).thenReturn(now.getMillis());
        _scanRequestManager.requestScanOnOrAfter("byrequest", now, "requester1");

        assertEquals(_scanRequestDAO.getRequestsForScan("byrequest-2017-11-20-12-00-00"), ImmutableSet.of(new ScanRequest("requester1", new Date(now.getMillis()))));
    }

    @Test (expectedExceptions = InvalidStashRequestException.class)
    public void testCreateRequestTooFarInPast() {
        DateTime now = ISODateTimeFormat.dateTime().parseDateTime("2017-11-20T11:30:00.000Z");
        when(_clock.millis()).thenReturn(now.getMillis());
        _scanRequestManager.requestScanOnOrAfter("byrequest", now.minusDays(1), "requester1");
    }

    @Test (expectedExceptions = InvalidStashRequestException.class)
    public void testUndoCreateRequestForNonExistentStash() {
        DateTime now = ISODateTimeFormat.dateTime().parseDateTime("2017-11-20T23:30:00.000Z");
        when(_clock.millis()).thenReturn(now.getMillis());
        _scanRequestManager.undoRequestForScanOnOrAfter("nosuchid", now, "requester1");
    }

    @Test (expectedExceptions = InvalidStashRequestException.class)
    public void testUndoCreateRequestForNoRequestRequiredStash() {
        DateTime now = ISODateTimeFormat.dateTime().parseDateTime("2017-11-20T23:30:00.000Z");
        when(_clock.millis()).thenReturn(now.getMillis());
        _scanRequestManager.requestScanOnOrAfter("always", now, "requester1");
    }

    @Test
    public void testUndoCreateRequestFutureStash() {
        DateTime now = ISODateTimeFormat.dateTime().parseDateTime("2017-11-20T11:30:00.000Z");
        _scanRequestDAO.requestScan("byrequest-2017-11-20-12-00-00", new ScanRequest("requester1", new Date(now.minusMinutes(1).getMillis())));
        when(_clock.millis()).thenReturn(now.getMillis());
        _scanRequestManager.undoRequestForScanOnOrAfter("byrequest", now, "requester1");

        assertEquals(_scanRequestDAO.getRequestsForScan("byrequest-2017-11-20-12-00-00"), ImmutableSet.of());
    }

    @Test (expectedExceptions = InvalidStashRequestException.class)
    public void testUndoCreateRequestTooFarInPast() {
        DateTime now = ISODateTimeFormat.dateTime().parseDateTime("2017-11-20T11:30:00.000Z");
        when(_clock.millis()).thenReturn(now.getMillis());
        _scanRequestManager.undoRequestForScanOnOrAfter("byrequest", now.minusDays(1), "requester1");
    }

    @Test
    public void testGetRequestsForNonExistentStash() {
        DateTime now = ISODateTimeFormat.dateTime().parseDateTime("2017-11-20T23:30:00.000Z");
        when(_clock.millis()).thenReturn(now.getMillis());
        Set<ScanRequest> requests = _scanRequestManager.getRequestsForScan("nosuchid", now);
        assertEquals(requests, ImmutableSet.of());
    }

    @Test
    public void testGetRequestsForNoRequestRequiredStash() {
        DateTime now = ISODateTimeFormat.dateTime().parseDateTime("2017-11-20T23:30:00.000Z");
        when(_clock.millis()).thenReturn(now.getMillis());
        Set<ScanRequest> requests = _scanRequestManager.getRequestsForScan("always", now);
        assertEquals(requests, ImmutableSet.of());
    }

    @Test
    public void testGetRequestsFutureStash() {
        DateTime now = ISODateTimeFormat.dateTime().parseDateTime("2017-11-20T11:30:00.000Z");
        ScanRequest request = new ScanRequest("requester1", new Date(now.minusMinutes(1).getMillis()));
        _scanRequestDAO.requestScan("byrequest-2017-11-20-12-00-00", request);
        when(_clock.millis()).thenReturn(now.getMillis());
        Set<ScanRequest> requests = _scanRequestManager.getRequestsForScan("byrequest", now);

        assertEquals(requests, ImmutableSet.of(request));
    }

    @Test
    public void testGetRequestsAtTimeOfStash() {
        DateTime now = ISODateTimeFormat.dateTime().parseDateTime("2017-11-20T12:00:00.000Z");
        ScanRequest request = new ScanRequest("requester1", new Date(now.minusMinutes(1).getMillis()));
        _scanRequestDAO.requestScan("byrequest-2017-11-20-12-00-00", request);
        when(_clock.millis()).thenReturn(now.getMillis());
        Set<ScanRequest> requests = _scanRequestManager.getRequestsForScan("byrequest", now);

        assertEquals(requests, ImmutableSet.of(request));
    }
}
