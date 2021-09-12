package com.bazaarvoice.emodb.web.scanner.scheduling;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.stash.StashUtil;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.ScanUploader;
import com.bazaarvoice.emodb.web.scanner.notifications.ScanCountListener;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.StashRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.longThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class ScanUploadSchedulingServiceTest {

    @DataProvider(name = "every10minutes")
    public static Object[][] oneDayAt10MinuteIntervals() {
        Object[][] dates = new Object[144][1];
        Instant date = Instant.ofEpochMilli(1470009600000L);

        for (int i = 0; i < 144; i++) {
            dates[i][0] = date;
            date = date.plus(Duration.ofMinutes(10));
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

    @Test
    public void testGetNextExecutionTime() {
        ScheduledDailyScanUpload dailyScanUpload =
                new ScheduledDailyScanUpload("daily", "18:00-06:00", DateTimeFormatter.ofPattern("'daily'-yyyy-MM-dd-HH-mm-ss").withZone(ZoneOffset.UTC),
                        ScanDestination.discard(), StashUtil.STASH_DIRECTORY_DATE_FORMAT,
                        ImmutableList.of("placement1"), 1, true, false, 1000000, Duration.ofMinutes(10));
        Instant tomorrowAtMidnightUTC = ZonedDateTime.from(new Date().toInstant().atZone(ZoneOffset.UTC))
                .plusDays(1)
                .withHour(0)
                .withMinute(0)
                .withSecond(0)
                .withNano(0)
                .toInstant();

        assertEquals(dailyScanUpload.getNextExecutionTimeAfter(Instant.now()), tomorrowAtMidnightUTC);
    }

    @Test(dataProvider = "every10minutes")
    public void testScanScheduled(Instant now) {

        ScanUploader scanUploader = mock(ScanUploader.class);
        StashRequestManager stashRequestManager = mock(StashRequestManager.class);
        ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        ScanCountListener scanCountListener = mock(ScanCountListener.class);

        Clock clock = Clock.fixed(Instant.ofEpochMilli(now.toEpochMilli()), ZoneId.systemDefault());

        // Schedule a scan for 1 hour in the past and 1 hour in the future
        Instant oneHourAgo = now.minus(Duration.ofHours(1));
        Instant oneHourFromNow = now.plus(Duration.ofHours(1));

        DateTimeFormatter format = DateTimeFormatter.ofPattern("HH:mmX").withZone(ZoneOffset.UTC);
        String pastTimeOfDay = format.format(oneHourAgo);
        String futureTimeOfDay = format.format(oneHourFromNow);

        ScheduledDailyScanUpload pastScanUpload =
                new ScheduledDailyScanUpload("daily", pastTimeOfDay, DateTimeFormatter.ofPattern("'past'-yyyyMMddHHmmss").withZone(ZoneOffset.UTC),
                        ScanDestination.discard(), DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC),
                        ImmutableList.of("placement1"), 1, true, false, 1000000, Duration.ofMinutes(10));
        ScheduledDailyScanUpload futureScanUpload =
                new ScheduledDailyScanUpload("daily", futureTimeOfDay, DateTimeFormatter.ofPattern("'future'-yyyyMMddHHmmss").withZone(ZoneOffset.UTC),
                        ScanDestination.discard(), DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC),
                        ImmutableList.of("placement2"), 1, true, false, 1000000, Duration.ofMinutes(10));

        List<ScheduledDailyScanUpload> scheduledScans = ImmutableList.of(pastScanUpload, futureScanUpload);

        ScanUploadSchedulingService.DelegateSchedulingService service =
                new ScanUploadSchedulingService.DelegateSchedulingService(scanUploader, stashRequestManager, scheduledScans, scanCountListener, clock);

        service.setExecutorService(executorService);
        service.initializeScans();

        // Verify the two scans were scheduled
        verify(executorService).schedule(
                any(Runnable.class),
                longThat(withinNSeconds(10, Duration.between(now, oneHourFromNow).toMillis())),
                eq(TimeUnit.MILLISECONDS));

        verify(executorService).schedule(
                any(Runnable.class),
                longThat(withinNSeconds(10, Duration.between(now, oneHourAgo.plus(Duration.ofDays(1))).toMillis())),
                eq(TimeUnit.MILLISECONDS));

        // Verify the pending scan updates were scheduled
        verify(executorService).scheduleAtFixedRate(
                any(Runnable.class),
                longThat(withinNSeconds(10, Duration.between(now, oneHourFromNow.minus(Duration.ofMinutes(45))).toMillis())),
                eq(TimeUnit.DAYS.toMillis(1)),
                eq(TimeUnit.MILLISECONDS));

        verify(executorService).scheduleAtFixedRate(
                any(Runnable.class),
                longThat(withinNSeconds(10, Duration.between(now, oneHourAgo.plus(Duration.ofDays(1)).minus(Duration.ofMinutes(45))).toMillis())),
                eq(TimeUnit.DAYS.toMillis(1)),
                eq(TimeUnit.MILLISECONDS));

        verifyNoMoreInteractions(executorService);
    }

    @Test(dataProvider = "every10minutes")
    public void testMissedScansStarted(Instant now) {
        ScanUploader scanUploader = mock(ScanUploader.class);
        ScanUploader.ScanAndUploadBuilder scanAndUploadBuilder = mock(ScanUploader.ScanAndUploadBuilder.class);
        when(scanUploader.scanAndUpload(anyString(), any(ScanOptions.class))).thenReturn(scanAndUploadBuilder);
        StashRequestManager stashRequestManager = mock(StashRequestManager.class);
        ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        ScanCountListener scanCountListener = mock(ScanCountListener.class);

        Clock clock = Clock.fixed(Instant.ofEpochMilli(now.toEpochMilli()), ZoneId.systemDefault());

        // 4 scans, only the first two of which should be considered missed
        Instant oneMinuteAgo = now.minus(Duration.ofMinutes(1));
        Instant nineMinutesAgo = now.minus(Duration.ofMinutes(9));
        Instant elevenMinutesAgo = now.minus(Duration.ofMinutes(11));
        Instant twoMinutesFromNow = now.plus(Duration.ofMinutes(2));

        DateTimeFormatter format = DateTimeFormatter.ofPattern("HH:mmX");
        List<ScheduledDailyScanUpload> scheduledScans = Lists.newArrayList();

        for (Instant scheduledTime : ImmutableList.of(oneMinuteAgo, nineMinutesAgo, elevenMinutesAgo, twoMinutesFromNow)) {
            String timeOfDay = format.format(scheduledTime.atZone(ZoneOffset.UTC));
            scheduledScans.add(
                    new ScheduledDailyScanUpload("daily", timeOfDay, DateTimeFormatter.ofPattern("'test'-yyyyMMddHHmmss").withZone(ZoneOffset.UTC),
                            ScanDestination.discard(), DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC),
                            ImmutableList.of("placement1"), 1, true, false, 1000000, Duration.ofMinutes(10)));
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

        String expectedScanId1 = DateTimeFormatter.ofPattern("'test'-yyyyMMddHHmmss").withZone(ZoneOffset.UTC).format(oneMinuteAgo);
        String expectedScanId9 = DateTimeFormatter.ofPattern("'test'-yyyyMMddHHmmss").withZone(ZoneOffset.UTC).format(nineMinutesAgo);

        // Called once each to to verify the scan does not exist
        verify(scanUploader).getStatus(expectedScanId1);
        verify(scanUploader).getStatus(expectedScanId9);

        // Each were actually scanned.  Don't concern over the exact options, that's covered in #testStartScheduledScan().
        verify(scanUploader).scanAndUpload(eq(expectedScanId1), any(ScanOptions.class));
        verify(scanUploader).scanAndUpload(eq(expectedScanId9), any(ScanOptions.class));
        verify(scanAndUploadBuilder, times(2)).start();

        verifyNoMoreInteractions(executorService, scanUploader);
    }

    @Test(dataProvider = "every10minutes")
    public void testStartScheduledScan(Instant now)
            throws Exception {
        DateTimeFormatter format = DateTimeFormatter.ofPattern("HH:mmX");
        String timeOfDay = format.format(now.atZone(ZoneOffset.UTC));

        ScanDestination destination = ScanDestination.to(URI.create("s3://bucket/path/to/root"));

        ScheduledDailyScanUpload scanUpload =
                new ScheduledDailyScanUpload("daily", timeOfDay, DateTimeFormatter.ofPattern("'test'-yyyyMMddHHmmss").withZone(ZoneOffset.UTC),
                        destination, DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC),
                        ImmutableList.of("placement1"), 1, true, false, 1000000, Duration.ofMinutes(10));

        ScanUploader.ScanAndUploadBuilder builder = mock(ScanUploader.ScanAndUploadBuilder.class);
        ScanUploader scanUploader = mock(ScanUploader.class);
        when(scanUploader.scanAndUpload(anyString(), any(ScanOptions.class))).thenReturn(builder);
        StashRequestManager stashRequestManager = mock(StashRequestManager.class);
        ScanCountListener scanCountListener = mock(ScanCountListener.class);

        Clock clock = Clock.fixed(Instant.ofEpochMilli(now.toEpochMilli()), ZoneId.systemDefault());

        ScanUploadSchedulingService.DelegateSchedulingService service =
                new ScanUploadSchedulingService.DelegateSchedulingService(scanUploader, stashRequestManager, ImmutableList.of(), scanCountListener, clock);

        service.startScheduledScan(scanUpload, now);

        String expectedScanId = DateTimeFormatter.ofPattern("'test'-yyyyMMddHHmmss").withZone(ZoneOffset.UTC).format(now);
        ScanDestination expectedDestination = ScanDestination.to(
                URI.create("s3://bucket/path/to/root/" + DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC).format(now)));

        // Called once to to verify the scan does not exist
        verify(scanUploader).getStatus(expectedScanId);

        verify(scanUploader).scanAndUpload(expectedScanId,
                new ScanOptions("placement1")
                        .addDestination(expectedDestination)
                        .setMaxConcurrentSubRangeScans(1)
                        .setScanByAZ(true));

        verify(builder).start();

        verifyNoMoreInteractions(scanUploader);
    }

    @Test(dataProvider = "every10minutes")
    public void testRepeatScheduledScan(Instant now)
            throws Exception {
        DateTimeFormatter format = DateTimeFormatter.ofPattern("HH:mmX");
        String timeOfDay = format.format(now.atZone(ZoneOffset.UTC));

        ScanDestination destination = ScanDestination.to(URI.create("s3://bucket/path/to/root"));

        ScheduledDailyScanUpload scanUpload =
                new ScheduledDailyScanUpload("daily", timeOfDay, DateTimeFormatter.ofPattern("'test'-yyyyMMddHHmmss").withZone(ZoneOffset.UTC),
                        destination, DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC),
                        ImmutableList.of("placement1"), 1, true, false, 1000000, Duration.ofMinutes(10));

        String expectedScanId = DateTimeFormatter.ofPattern("'test'-yyyyMMddHHmmss").withZone(ZoneOffset.UTC).format(now);

        ScanUploader scanUploader = mock(ScanUploader.class);
        when(scanUploader.getStatus(expectedScanId)).thenReturn(new ScanStatus(
                expectedScanId, new ScanOptions("placement1"), true, false, new Date(), ImmutableList.of(),
                ImmutableList.of(), ImmutableList.of()));

        StashRequestManager stashRequestManager = mock(StashRequestManager.class);
        ScanCountListener scanCountListener = mock(ScanCountListener.class);

        Clock clock = Clock.fixed(Instant.ofEpochMilli(now.toEpochMilli()), ZoneId.systemDefault());

        ScanUploadSchedulingService.DelegateSchedulingService service =
                new ScanUploadSchedulingService.DelegateSchedulingService(scanUploader, stashRequestManager, ImmutableList.of(), scanCountListener, clock);

        try {
            service.startScheduledScan(scanUpload, now);
            fail("RepeatScanException not thrown");
        } catch (ScanUploadSchedulingService.RepeatScanException e) {
            // expected
        }

        verify(scanUploader).getStatus(expectedScanId);
        verifyNoMoreInteractions(scanUploader);
    }

    @Test(dataProvider = "every10minutes")
    public void testParticipationNotification(Instant now)
            throws Exception {
        StashStateListener stashStateListener = mock(StashStateListener.class);
        LifeCycleRegistry lifecycle = mock(LifeCycleRegistry.class);

        ScheduledExecutorService participationExecutorService = spy(Executors.newScheduledThreadPool(1));

        Clock clock = Clock.fixed(Instant.ofEpochMilli(now.toEpochMilli()), ZoneId.systemDefault());

        // Start one hour in the future
        String startTime = DateTimeFormatter.ofPattern("HH:mmX").withZone(ZoneOffset.UTC).format(now.plus(Duration.ofHours(1)));

        ScheduledDailyScanUpload upload = new ScheduledDailyScanUpload(
                "daily", startTime, DateTimeFormatter.ISO_INSTANT, ScanDestination.discard(), DateTimeFormatter.ISO_INSTANT,
                ImmutableList.of("catalog_global:cat"), 5, true, false, 1000000, Duration.ofMinutes(10));

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
                    longThat(withinNSeconds(60, Duration.ofHours(1).toMillis())),
                    eq(Duration.ofDays(1).toMillis()),
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
    public void testByRequestStashStartsWhenRequested(Instant now, boolean requested) {
        ScanUploader scanUploader = mock(ScanUploader.class);
        StashRequestManager stashRequestManager = mock(StashRequestManager.class);
        ScheduledExecutorService executorService = mock(ScheduledExecutorService.class);
        ScanCountListener scanCountListener = mock(ScanCountListener.class);

        final AtomicReference<Instant> clockTime = new AtomicReference<>(now);
        Clock clock = mock(Clock.class);
        when(clock.instant()).thenAnswer(invocationOnMock -> clockTime.get());

        // Schedule a scan for 1 hour in the past and 1 hour in the future
        Instant nowMinus1Hour = now.minus(Duration.ofHours(1));
        Instant nowPlus15Minutes = now.plus(Duration.ofMinutes(15));
        Instant nowPlus1Hour = now.plus(Duration.ofHours(1));
        Instant nowPlus22Hours15Minutes = now.plus(Duration.ofHours(22)).plus(Duration.ofMinutes(15));
        Instant nowPlus23Hours = now.plus(Duration.ofHours(23));

        DateTimeFormatter format = DateTimeFormatter.ofPattern("HH:mmX");
        String pastTimeOfDay = format.format(nowMinus1Hour.atZone(ZoneOffset.UTC));
        String futureTimeOfDay = format.format(nowPlus1Hour.atZone(ZoneOffset.UTC));

        ScheduledDailyScanUpload pastScanUpload =
                new ScheduledDailyScanUpload("past", pastTimeOfDay, DateTimeFormatter.ofPattern("'past'-yyyyMMddHHmmss").withZone(ZoneOffset.UTC),
                        ScanDestination.discard(), DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC),
                        ImmutableList.of("placement1"), 1, true, true, 1000000, Duration.ofMinutes(10));
        ScheduledDailyScanUpload futureScanUpload =
                new ScheduledDailyScanUpload("future", futureTimeOfDay, DateTimeFormatter.ofPattern("'future'-yyyyMMddHHmmss").withZone(ZoneOffset.UTC),
                        ScanDestination.discard(), DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone(ZoneOffset.UTC),
                        ImmutableList.of("placement2"), 1, true, true, 1000000, Duration.ofMinutes(10));

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
                longThat(withinNSeconds(10, Duration.between(now, nowPlus15Minutes).toMillis())),
                eq(TimeUnit.DAYS.toMillis(1)),
                eq(TimeUnit.MILLISECONDS));

        verify(executorService).schedule(
                futureStartRunnable.capture(),
                longThat(withinNSeconds(10, Duration.between(now, nowPlus1Hour).toMillis())),
                eq(TimeUnit.MILLISECONDS));

        verify(executorService).scheduleAtFixedRate(
                pastPendingRunnable.capture(),
                longThat(withinNSeconds(10, Duration.between(now, nowPlus22Hours15Minutes).toMillis())),
                eq(TimeUnit.DAYS.toMillis(1)),
                eq(TimeUnit.MILLISECONDS));

        verify(executorService).schedule(
                pastStartRunnable.capture(),
                longThat(withinNSeconds(10, Duration.between(now, nowPlus23Hours).toMillis())),
                eq(TimeUnit.MILLISECONDS));

        Set<StashRequest> requests = requested ?
                ImmutableSet.of(new StashRequest("key", new Date(now.toEpochMilli()))) :
                ImmutableSet.of();

        when(stashRequestManager.getRequestsForStash("future", nowPlus1Hour)).thenReturn(requests);
        when(stashRequestManager.getRequestsForStash("past", nowPlus23Hours)).thenReturn(requests);
        when(scanUploader.getStatus(anyString())).thenReturn(null);

        String futureScanId = futureScanUpload.getScanIdFormat().format(nowPlus1Hour);
        String pastScanId = pastScanUpload.getScanIdFormat().format(nowPlus23Hours);

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

    private ArgumentMatcher<Long> withinNSeconds(final int seconds, final long expected) {
        return new ArgumentMatcher<Long>() {
            @Override
            public boolean matches(Long item) {
                return Math.abs(item - expected) < TimeUnit.SECONDS.toMillis(seconds);
            }

            @Override
            public String toString() {
                return "Within " + seconds + " seconds of " + expected;
            }
        };
    }
}
