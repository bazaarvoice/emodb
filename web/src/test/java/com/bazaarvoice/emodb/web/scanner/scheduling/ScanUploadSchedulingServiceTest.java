package com.bazaarvoice.emodb.web.scanner.scheduling;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.plugin.stash.StashStateListener;
import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.ScanUploader;
import com.bazaarvoice.emodb.web.scanner.notifications.ScanCountListener;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanRangeStatus;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.google.common.collect.ImmutableList;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.longThat;
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

    @Test(dataProvider = "every10minutes")
    public void testScanScheduled(DateTime now)
            throws Exception {

        ScanUploader scanUploader = mock(ScanUploader.class);
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
                new ScheduledDailyScanUpload(pastTimeOfDay, DateTimeFormat.forPattern("'past'-yyyyMMddHHmmss").withZoneUTC(),
                        ScanDestination.discard(), DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC(),
                        ImmutableList.of("placement1"), 1, true);
        ScheduledDailyScanUpload futureScanUpload =
                new ScheduledDailyScanUpload(futureTimeOfDay, DateTimeFormat.forPattern("'future'-yyyyMMddHHmmss").withZoneUTC(),
                        ScanDestination.discard(),DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC(),
                        ImmutableList.of("placement2"), 1, true);

        List<ScheduledDailyScanUpload> scheduledScans = ImmutableList.of(pastScanUpload, futureScanUpload);

        ScanUploadSchedulingService.DelegateSchedulingService service =
                new ScanUploadSchedulingService.DelegateSchedulingService(scanUploader, scheduledScans, scanCountListener, clock);

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
                    new ScheduledDailyScanUpload(timeOfDay, DateTimeFormat.forPattern("'test'-yyyyMMddHHmmss").withZoneUTC(),
                            ScanDestination.discard(), DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC(),
                            ImmutableList.of("placement1"), 1, true));
        }

        ScanUploadSchedulingService.DelegateSchedulingService service =
                new ScanUploadSchedulingService.DelegateSchedulingService(scanUploader, scheduledScans, scanCountListener, clock);

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
                new ScheduledDailyScanUpload(timeOfDay, DateTimeFormat.forPattern("'test'-yyyyMMddHHmmss").withZoneUTC(),
                        destination, DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC(),
                        ImmutableList.of("placement1"), 1, true);

        ScanUploader scanUploader = mock(ScanUploader.class);
        ScanCountListener scanCountListener = mock(ScanCountListener.class);

        Clock clock = Clock.fixed(Instant.ofEpochMilli(now.getMillis()), ZoneId.systemDefault());

        ScanUploadSchedulingService.DelegateSchedulingService service =
                new ScanUploadSchedulingService.DelegateSchedulingService(scanUploader, ImmutableList.<ScheduledDailyScanUpload>of(), scanCountListener, clock);

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
                new ScheduledDailyScanUpload(timeOfDay, DateTimeFormat.forPattern("'test'-yyyyMMddHHmmss").withZoneUTC(),
                        destination, DateTimeFormat.forPattern("yyyyMMddHHmmss").withZoneUTC(),
                        ImmutableList.of("placement1"), 1, true);

        String expectedScanId = DateTimeFormat.forPattern("'test'-yyyyMMddHHmmss").withZoneUTC().print(now);

        ScanUploader scanUploader = mock(ScanUploader.class);
        when(scanUploader.getStatus(expectedScanId)).thenReturn(new ScanStatus(
                expectedScanId, new ScanOptions("placement1"), true, false, new Date(), ImmutableList.<ScanRangeStatus>of(),
                ImmutableList.<ScanRangeStatus>of(), ImmutableList.<ScanRangeStatus>of()));

        ScanCountListener scanCountListener = mock(ScanCountListener.class);

        Clock clock = Clock.fixed(Instant.ofEpochMilli(now.getMillis()), ZoneId.systemDefault());

        ScanUploadSchedulingService.DelegateSchedulingService service =
                new ScanUploadSchedulingService.DelegateSchedulingService(scanUploader, ImmutableList.<ScheduledDailyScanUpload>of(), scanCountListener, clock);

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
                startTime, DateTimeFormat.longDateTime(), ScanDestination.discard(), DateTimeFormat.longDateTime(),
                ImmutableList.of("catalog_global:cat"), 5, true);

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
