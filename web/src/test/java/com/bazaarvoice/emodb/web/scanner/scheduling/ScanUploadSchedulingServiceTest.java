package com.bazaarvoice.emodb.web.scanner.scheduling;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.ScanUploader;
import com.bazaarvoice.emodb.web.scanner.notifications.ScanCountListener;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanRangeStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.StashRequest;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.longThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

public class ScanUploadSchedulingServiceTest {

    @DataProvider(name = "every10minutes")
    public static Object[][] oneDayAt10MinuteIntervals() {
        Object[][] dates = new Object[144][1];
        DateTime date = new DateTime(1470009600000L).withZone(DateTimeZone.UTC);

        for (int i=0; i < 144; i++) {
            dates[i][0] = date;
            date = date.plusMinutes(10);
        }

        return dates;
    }

    @DataProvider(name = "every10minutesByRequest")
    public static Object[][] oneDayAt10MinuteIntervalsByRequest() {
        Object[][] dates = oneDayAt10MinuteIntervals();
        Object[][] dateRequests = new Object[dates.length * 2][2];

        int idx = 0;
        for (Object[] args : dates) {
            dateRequests[idx][0] = args[0];
            dateRequests[idx++][1] = true;
            dateRequests[idx][0] = args[0];
            dateRequests[idx++][1] = false;
        }

        return dateRequests;
    }

    @Test(dataProvider = "every10minutes")
    public void testScanScheduled(DateTime now)
            throws Exception {

        ScanUploader scanUploader = mock(ScanUploader.class);
        StashRequestManager stashRequestManager = mock(StashRequestManager.class);
        ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        ScanCountListener scanCountListener = mock(ScanCountListener.class);

        Clock clock = Clock.fixed(Instant.ofEpochMilli(now.getMillis()), ZoneId.systemDefault());

        // Schedule a scan for 1 hour in the past and 1 hour in the future
        DateTime oneHourAgo = now.minusHours(1);
        DateTime oneHourFromNow = now.plusHours(1);

        DateTimeFormatter format = DateTimeFormat.forPattern("HH:mmZ");
        String pastTimeOfDay = format.print(oneHourAgo);
        String futureTimeOfDay = format.print(oneHourFromNow);

        ScheduledDailyScanUpload pastScanUpload =
                new ScheduledDailyScanUpload("daily", pastTimeOfDay, DateTimeFormat.forPattern("'past'-yyyyMMddHHmmss").withZoneUTC(),
                        ScanDestination.discard(), DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC(),
                        ImmutableList.of("placement1"), 1, true, false);
        ScheduledDailyScanUpload futureScanUpload =
                new ScheduledDailyScanUpload("daily", futureTimeOfDay, DateTimeFormat.forPattern("'future'-yyyyMMddHHmmss").withZoneUTC(),
                        ScanDestination.discard(),DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC(),
                        ImmutableList.of("placement2"), 1, true, false);

        List<ScheduledDailyScanUpload> scheduledScans = ImmutableList.of(pastScanUpload, futureScanUpload);

        ScanUploadSchedulingService.DelegateSchedulingService service =
                new ScanUploadSchedulingService.DelegateSchedulingService(scanUploader, stashRequestManager, scheduledScans, scanCountListener, clock);

        service.setExecutorService(executorService);
        service.initializeScans();

        // Verify the two scans were scheduled
        verify(executorService).schedule(
                any(Runnable.class),
                longThat(withinNSeconds(10, oneHourFromNow.minus(now.getMillis()).getMillis())),
                eq(TimeUnit.MILLISECONDS));

        verify(executorService).schedule(
                any(Runnable.class),
                longThat(withinNSeconds(10, oneHourAgo.plusDays(1).minus(now.getMillis()).getMillis())),
                eq(TimeUnit.MILLISECONDS));

        // Verify the pending scan updates were scheduled
        verify(executorService).scheduleAtFixedRate(
                any(Runnable.class),
                longThat(withinNSeconds(10, oneHourFromNow.minusMinutes(45).minus(now.getMillis()).getMillis())),
                eq(TimeUnit.DAYS.toMillis(1)),
                eq(TimeUnit.MILLISECONDS));

        verify(executorService).scheduleAtFixedRate(
                any(Runnable.class),
                longThat(withinNSeconds(10, oneHourAgo.plusDays(1).minusMinutes(45).minus(now.getMillis()).getMillis())),
                eq(TimeUnit.DAYS.toMillis(1)),
                eq(TimeUnit.MILLISECONDS));

        verifyNoMoreInteractions(executorService);
    }

    @Test(dataProvider = "every10minutes")
    public void testMissedScansStarted(DateTime now) {
        ScanUploader scanUploader = mock(ScanUploader.class);
        StashRequestManager stashRequestManager = mock(StashRequestManager.class);
        ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        ScanCountListener scanCountListener = mock(ScanCountListener.class);

        Clock clock = Clock.fixed(Instant.ofEpochMilli(now.getMillis()), ZoneId.systemDefault());

        // 4 scans, only the first two of which should be considered missed
        DateTime oneMinuteAgo = now.minusMinutes(1);
        DateTime nineMinutesAgo = now.minusMinutes(9);
        DateTime elevenMinutesAgo = now.minusMinutes(11);
        DateTime twoMinutesFromNow = now.plusMinutes(2);

        DateTimeFormatter format = DateTimeFormat.forPattern("HH:mmZ");
        List<ScheduledDailyScanUpload> scheduledScans = Lists.newArrayList();

        for (DateTime scheduledTime : ImmutableList.of(oneMinuteAgo, nineMinutesAgo, elevenMinutesAgo, twoMinutesFromNow)) {
            String timeOfDay = format.print(scheduledTime);
            scheduledScans.add(
                    new ScheduledDailyScanUpload("daily", timeOfDay, DateTimeFormat.forPattern("'test'-yyyyMMddHHmmss").withZoneUTC(),
                            ScanDestination.discard(), DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC(),
                            ImmutableList.of("placement1"), 1, true, false));
        }

        ScanUploadSchedulingService.DelegateSchedulingService service =
                new ScanUploadSchedulingService.DelegateSchedulingService(scanUploader, stashRequestManager, scheduledScans, scanCountListener, clock);

        service.setExecutorService(executorService);
        service.initializeScans();

        // All 4 scans and their pending scan updates were scheduled.  Don't concern over exact times,
        // that's covered in #testScanScheduled().
        verify(executorService, times(4)).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
        verify(executorService, times(4)).scheduleAtFixedRate(any(Runnable.class), anyLong(),
                eq(TimeUnit.DAYS.toMillis(1)), eq(TimeUnit.MILLISECONDS));

        String expectedScanId1 = DateTimeFormat.forPattern("'test'-yyyyMMddHHmmss").withZoneUTC().print(oneMinuteAgo);
        String expectedScanId9 = DateTimeFormat.forPattern("'test'-yyyyMMddHHmmss").withZoneUTC().print(nineMinutesAgo);

        // Called once each to to verify the scan does not exist
        verify(scanUploader).getStatus(expectedScanId1);
        verify(scanUploader).getStatus(expectedScanId9);

        // Each were actually scanned.  Don't concern over the exact options, that's covered in #testStartScheduledScan().
        verify(scanUploader).scanAndUpload(eq(expectedScanId1), any(ScanOptions.class));
        verify(scanUploader).scanAndUpload(eq(expectedScanId9), any(ScanOptions.class));

        verifyNoMoreInteractions(executorService, scanUploader);
    }

    @Test(dataProvider = "every10minutes")
    public void testStartScheduledScan(DateTime now)
            throws Exception {
        DateTimeFormatter format = DateTimeFormat.forPattern("HH:mmZ");
        String timeOfDay = format.print(now);

        ScanDestination destination = ScanDestination.to(URI.create("s3://bucket/path/to/root"));

        ScheduledDailyScanUpload scanUpload =
                new ScheduledDailyScanUpload("daily", timeOfDay, DateTimeFormat.forPattern("'test'-yyyyMMddHHmmss").withZoneUTC(),
                        destination, DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC(),
                        ImmutableList.of("placement1"), 1, true, false);

        ScanUploader scanUploader = mock(ScanUploader.class);
        StashRequestManager stashRequestManager = mock(StashRequestManager.class);
        ScanCountListener scanCountListener = mock(ScanCountListener.class);

        Clock clock = Clock.fixed(Instant.ofEpochMilli(now.getMillis()), ZoneId.systemDefault());

        ScanUploadSchedulingService.DelegateSchedulingService service =
                new ScanUploadSchedulingService.DelegateSchedulingService(scanUploader, stashRequestManager, ImmutableList.<ScheduledDailyScanUpload>of(), scanCountListener, clock);

        service.startScheduledScan(scanUpload, now);

        String expectedScanId = DateTimeFormat.forPattern("'test'-yyyyMMddHHmmss").withZoneUTC().print(now);
        ScanDestination expectedDestination = ScanDestination.to(
                URI.create("s3://bucket/path/to/root/" + DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC().print(now)));

        // Called once to to verify the scan does not exist
        verify(scanUploader).getStatus(expectedScanId);

        verify(scanUploader).scanAndUpload(expectedScanId,
                new ScanOptions("placement1")
                        .addDestination(expectedDestination)
                        .setMaxConcurrentSubRangeScans(1)
                        .setScanByAZ(true));

        verifyNoMoreInteractions(scanUploader);
    }

    @Test(dataProvider = "every10minutes")
    public void testRepeatScheduledScan(DateTime now)
            throws Exception {
        DateTimeFormatter format = DateTimeFormat.forPattern("HH:mmZ");
        String timeOfDay = format.print(now);

        ScanDestination destination = ScanDestination.to(URI.create("s3://bucket/path/to/root"));

        ScheduledDailyScanUpload scanUpload =
                new ScheduledDailyScanUpload("daily", timeOfDay, DateTimeFormat.forPattern("'test'-yyyyMMddHHmmss").withZoneUTC(),
                        destination, DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC(),
                        ImmutableList.of("placement1"), 1, true, false);

        String expectedScanId = DateTimeFormat.forPattern("'test'-yyyyMMddHHmmss").withZoneUTC().print(now);

        ScanUploader scanUploader = mock(ScanUploader.class);
        when(scanUploader.getStatus(expectedScanId)).thenReturn(new ScanStatus(
                expectedScanId, new ScanOptions("placement1"), true, false, new Date(), ImmutableList.<ScanRangeStatus>of(),
                ImmutableList.<ScanRangeStatus>of(), ImmutableList.<ScanRangeStatus>of()));

        StashRequestManager stashRequestManager = mock(StashRequestManager.class);
        ScanCountListener scanCountListener = mock(ScanCountListener.class);

        Clock clock = Clock.fixed(Instant.ofEpochMilli(now.getMillis()), ZoneId.systemDefault());

        ScanUploadSchedulingService.DelegateSchedulingService service =
                new ScanUploadSchedulingService.DelegateSchedulingService(scanUploader, stashRequestManager, ImmutableList.<ScheduledDailyScanUpload>of(), scanCountListener, clock);

        try {
            service.startScheduledScan(scanUpload, now);
            fail("RepeatScanException not thrown");
        } catch (ScanUploadSchedulingService.RepeatScanException e) {
            // expected
        }

        verify(scanUploader).getStatus(expectedScanId);
        verifyNoMoreInteractions(scanUploader);
    }

    @SuppressWarnings("unchecked")
    @Test(dataProvider = "every10minutes")
    public void testParticipationNotification(DateTime now)
            throws Exception {
        StashStateListener stashStateListener = mock(StashStateListener.class);
        LifeCycleRegistry lifecycle = mock(LifeCycleRegistry.class);

        ScheduledExecutorService participationExecutorService = spy(Executors.newScheduledThreadPool(1));

        Clock clock = Clock.fixed(Instant.ofEpochMilli(now.getMillis()), ZoneId.systemDefault());

        // Start one hour in the future
        String startTime = DateTimeFormat.forPattern("HH:mmZ").withZoneUTC().print(now.plusHours(1));

        ScheduledDailyScanUpload upload = new ScheduledDailyScanUpload(
                "daily", startTime, DateTimeFormat.longDateTime(), ScanDestination.discard(), DateTimeFormat.longDateTime(),
                ImmutableList.of("catalog_global:cat"), 5, true, false);

        ScanParticipationService service = new ScanParticipationService(
                ImmutableList.of(upload), stashStateListener, lifecycle, clock);

        service.setScheduledExecutorService(participationExecutorService);

        try {
            service.start();

            // Verify the runnable was scheduled one hour in the future, with 60 seconds of slop to account for the actual
            // scheduling taking place at anytime within the current minute.
            ArgumentCaptor<Runnable> runnable = ArgumentCaptor.forClass(Runnable.class);
            verify(participationExecutorService).scheduleAtFixedRate(
                    runnable.capture(),
                    longThat(withinNSeconds(60, Duration.standardHours(1).getMillis())),
                    eq(Duration.standardDays(1).getMillis()),
                    eq(TimeUnit.MILLISECONDS));


            // Verify running the scheduled executable announces the scan participation.
            verify(stashStateListener, never()).announceStashParticipation();
            runnable.getValue().run();
            verify(stashStateListener, times(1)).announceStashParticipation();
        } finally {
            service.stop();
            participationExecutorService.shutdownNow();
        }

    }

    @Test(dataProvider = "every10minutesByRequest")
    public void testByRequestStashStartsWhenRequested(DateTime now, boolean requested) {
        ScanUploader scanUploader = mock(ScanUploader.class);
        StashRequestManager stashRequestManager = mock(StashRequestManager.class);
        ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        ScanCountListener scanCountListener = mock(ScanCountListener.class);

        final AtomicReference<DateTime> clockTime = new AtomicReference<>(now);
        Clock clock = mock(Clock.class);
        when(clock.millis()).thenAnswer(invocationOnMock -> clockTime.get().getMillis());

        // Schedule a scan for 1 hour in the past and 1 hour in the future
        DateTime nowMinus1Hour = now.minusHours(1);
        DateTime nowPlus15Minutes = now.plusMinutes(15);
        DateTime nowPlus1Hour = now.plusHours(1);
        DateTime nowPlus22Hours15Minutes = now.plusHours(22).plusMinutes(15);
        DateTime nowPlus23Hours = now.plusHours(23);

        DateTimeFormatter format = DateTimeFormat.forPattern("HH:mmZ");
        String pastTimeOfDay = format.print(nowMinus1Hour);
        String futureTimeOfDay = format.print(nowPlus1Hour);

        ScheduledDailyScanUpload pastScanUpload =
                new ScheduledDailyScanUpload("past", pastTimeOfDay, DateTimeFormat.forPattern("'past'-yyyyMMddHHmmss").withZoneUTC(),
                        ScanDestination.discard(), DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC(),
                        ImmutableList.of("placement1"), 1, true, true);
        ScheduledDailyScanUpload futureScanUpload =
                new ScheduledDailyScanUpload("future", futureTimeOfDay, DateTimeFormat.forPattern("'future'-yyyyMMddHHmmss").withZoneUTC(),
                        ScanDestination.discard(),DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC(),
                        ImmutableList.of("placement2"), 1, true, true);

        List<ScheduledDailyScanUpload> scheduledScans = ImmutableList.of(pastScanUpload, futureScanUpload);

        ScanUploadSchedulingService.DelegateSchedulingService service =
                new ScanUploadSchedulingService.DelegateSchedulingService(scanUploader, stashRequestManager, scheduledScans, scanCountListener, clock);

        service.setExecutorService(executorService);
        service.initializeScans();

        // Four executions should have been scheduled, one for each scheduled scan: the pending check and the scan start
        ArgumentCaptor<Runnable> futurePendingRunnable = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Runnable> futureStartRunnable = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Runnable> pastPendingRunnable = ArgumentCaptor.forClass(Runnable.class);
        ArgumentCaptor<Runnable> pastStartRunnable = ArgumentCaptor.forClass(Runnable.class);

        verify(executorService).scheduleAtFixedRate(
                futurePendingRunnable.capture(),
                longThat(withinNSeconds(10, nowPlus15Minutes.minus(now.getMillis()).getMillis())),
                eq(TimeUnit.DAYS.toMillis(1)),
                eq(TimeUnit.MILLISECONDS));

        verify(executorService).schedule(
                futureStartRunnable.capture(),
                longThat(withinNSeconds(10, nowPlus1Hour.minus(now.getMillis()).getMillis())),
                eq(TimeUnit.MILLISECONDS));

        verify(executorService).scheduleAtFixedRate(
                pastPendingRunnable.capture(),
                longThat(withinNSeconds(10, nowPlus22Hours15Minutes.minus(now.getMillis()).getMillis())),
                eq(TimeUnit.DAYS.toMillis(1)),
                eq(TimeUnit.MILLISECONDS));

        verify(executorService).schedule(
                pastStartRunnable.capture(),
                longThat(withinNSeconds(10, nowPlus23Hours.minus(now.getMillis()).getMillis())),
                eq(TimeUnit.MILLISECONDS));

        Set<StashRequest> requests = requested ?
                ImmutableSet.of(new StashRequest("key", new Date(now.getMillis()))) :
                ImmutableSet.of();

        when(stashRequestManager.getRequestsForStash("future", nowPlus1Hour)).thenReturn(requests);
        when(stashRequestManager.getRequestsForStash("past", nowPlus23Hours)).thenReturn(requests);
        when(scanUploader.getStatus(anyString())).thenReturn(null);

        String futureScanId = futureScanUpload.getScanIdFormat().print(nowPlus1Hour.getMillis());
        String pastScanId = pastScanUpload.getScanIdFormat().print(nowPlus23Hours.getMillis());

        // Execute all of the scheduled runnables
        clockTime.set(nowPlus15Minutes);
        futurePendingRunnable.getValue().run();
        clockTime.set(nowPlus1Hour);
        futureStartRunnable.getValue().run();
        clockTime.set(nowPlus22Hours15Minutes);
        pastPendingRunnable.getValue().run();
        clockTime.set(nowPlus23Hours);
        pastStartRunnable.getValue().run();

        if (requested) {
            // Pending scans was initially zero then incremented and decremented once per scan
            verify(scanCountListener, times(3)).pendingScanCountChanged(0);
            verify(scanCountListener, times(2)).pendingScanCountChanged(1);

            // Each start runnable should have started a scan then scheduled another run 24 hours later
            verify(scanUploader).scanAndUpload(futureScanId, new ScanOptions("placement2").addDestination(ScanDestination.discard()).setMaxConcurrentSubRangeScans(1).setScanByAZ(true));
            verify(scanUploader).scanAndUpload(pastScanId, new ScanOptions("placement1").addDestination(ScanDestination.discard()).setMaxConcurrentSubRangeScans(1).setScanByAZ(true));

            // Verify incidental calls made while verifying the status of the scan
            verify(scanUploader).getStatus(futureScanId);
            verify(scanUploader).getStatus(pastScanId);

        } else {
            // Pending scans was initially zero then unchanged
            verify(scanCountListener).pendingScanCountChanged(0);

            // The pending scan checks should have each scheduled a recheck in 30 seconds
            verify(executorService, times(2)).schedule(any(Runnable.class), eq(30L), eq(TimeUnit.SECONDS));
        }

        // Each start runnable should have scheduled another run 24 hours later
        verify(executorService, times(2)).schedule(any(Runnable.class), eq(TimeUnit.DAYS.toMillis(1)), eq(TimeUnit.MILLISECONDS));

        // Verify incidental calls made while checking requests for the scan
        verify(stashRequestManager, atLeastOnce()).getRequestsForStash("future", nowPlus1Hour);
        verify(stashRequestManager, atLeastOnce()).getRequestsForStash("past", nowPlus23Hours);

        verifyNoMoreInteractions(executorService, stashRequestManager, scanUploader, scanCountListener);
    }

    private Matcher<Long> withinNSeconds(final int seconds, final long expected) {
        return new BaseMatcher<Long>() {
            @Override
            public boolean matches(Object item) {
                return Math.abs(((Long) item) - expected) < TimeUnit.SECONDS.toMillis(seconds);
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("Within 10 seconds of ").appendValue(expected);
            }
        };
    }
}
