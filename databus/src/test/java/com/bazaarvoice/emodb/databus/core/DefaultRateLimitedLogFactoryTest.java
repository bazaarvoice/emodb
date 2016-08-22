package com.bazaarvoice.emodb.databus.core;

import io.dropwizard.util.Duration;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;

public class DefaultRateLimitedLogFactoryTest {
    @Test
    public void testSingleError() {
        Logger log = mock(Logger.class);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        RateLimitedLog rateLimitedLog =
                new DefaultRateLimitedLogFactory(executor, Duration.days(1))
                        .from(log);
        verify(executor).scheduleWithFixedDelay(Matchers.<Runnable>any(), eq(1L), eq(1L), eq(TimeUnit.MINUTES));
        Throwable t = new Throwable();

        rateLimitedLog.error(t, "Test error: {}", "first!");

        verify(log).error("Test error: first!", t);
        verify(executor).schedule(Matchers.<Runnable>any(), eq(1L), eq(TimeUnit.DAYS));
        verifyNoMoreInteractions(log, executor);
    }

    @Test
    public void testMultipleErrors() {
        Logger log = mock(Logger.class);
        ScheduledExecutorService executor = mock(ScheduledExecutorService.class);
        Duration seconds = Duration.seconds(30);
        RateLimitedLog rateLimitedLog =
                new DefaultRateLimitedLogFactory(executor, seconds)
                        .from(log);
        verify(executor).scheduleWithFixedDelay(Matchers.<Runnable>any(), eq(1L), eq(1L), eq(TimeUnit.MINUTES));
        Throwable t1 = new Throwable();
        Throwable t2 = new Throwable();
        Throwable t3 = new Throwable();

        rateLimitedLog.error(t1, "Test error: {}", "first!");
        rateLimitedLog.error(t2, "Test error: {}", "second!");
        rateLimitedLog.error(t3, "Test error: {}", "third!");

        // Check that the first error gets logged immediately.
        verify(log).error("Test error: first!", t1);
        ArgumentCaptor<Runnable> captor1 = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).schedule(captor1.capture(), eq(30L), eq(TimeUnit.SECONDS));
        verifyNoMoreInteractions(log, executor);
        reset(executor);

        // Simulate the scheduled executor service running at the scheduled time after accumulating two more errors.
        captor1.getValue().run();

        verify(log).error("Encountered {} {} within the last {}: {}", 2L, "errors", Duration.seconds(30), "Test error: third!", t3);
        ArgumentCaptor<Runnable> captor2 = ArgumentCaptor.forClass(Runnable.class);
        verify(executor).schedule(captor2.capture(), eq(30L), eq(TimeUnit.SECONDS));
        verifyNoMoreInteractions(log, executor);

        // Simulate the scheduled executor service running at the scheduled time, this time with no errors to report.
        captor2.getValue().run();

        verifyNoMoreInteractions(log, executor);
    }

    @Test
    public void testDurationToString() {
        assertEquals("30 seconds", Duration.seconds(30).toString());
    }
}
