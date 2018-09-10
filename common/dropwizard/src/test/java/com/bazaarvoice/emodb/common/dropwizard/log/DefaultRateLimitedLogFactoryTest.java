package com.bazaarvoice.emodb.common.dropwizard.log;

import io.dropwizard.util.Duration;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DefaultRateLimitedLogFactoryTest {
    @Test
    public void testSingleError() {
        Logger log = Mockito.mock(Logger.class);
        ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
        RateLimitedLog rateLimitedLog =
                new DefaultRateLimitedLogFactory(executor, Duration.days(1))
                        .from(log);
        Mockito.verify(executor).scheduleWithFixedDelay(Matchers.<Runnable>any(), Matchers.eq(1L), Matchers.eq(1L), Matchers.eq(TimeUnit.MINUTES));
        Throwable t = new Throwable();

        rateLimitedLog.error(t, "Test error: {}", "first!");

        Mockito.verify(log).error("Test error: first!", t);
        Mockito.verify(executor).schedule(Matchers.<Runnable>any(), Matchers.eq(1L), Matchers.eq(TimeUnit.DAYS));
        Mockito.verifyNoMoreInteractions(log, executor);
    }

    @Test
    public void testMultipleErrors() {
        Logger log = Mockito.mock(Logger.class);
        ScheduledExecutorService executor = Mockito.mock(ScheduledExecutorService.class);
        Duration seconds = Duration.seconds(30);
        RateLimitedLog rateLimitedLog =
                new DefaultRateLimitedLogFactory(executor, seconds)
                        .from(log);
        Mockito.verify(executor).scheduleWithFixedDelay(Matchers.<Runnable>any(), Matchers.eq(1L), Matchers.eq(1L), Matchers.eq(TimeUnit.MINUTES));
        Throwable t1 = new Throwable();
        Throwable t2 = new Throwable();
        Throwable t3 = new Throwable();

        rateLimitedLog.error(t1, "Test error: {}", "first!");
        rateLimitedLog.error(t2, "Test error: {}", "second!");
        rateLimitedLog.error(t3, "Test error: {}", "third!");

        // Check that the first error gets logged immediately.
        Mockito.verify(log).error("Test error: first!", t1);
        ArgumentCaptor<Runnable> captor1 = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(executor).schedule(captor1.capture(), Matchers.eq(30L), Matchers.eq(TimeUnit.SECONDS));
        Mockito.verifyNoMoreInteractions(log, executor);
        Mockito.reset(executor);

        // Simulate the scheduled executor service running at the scheduled time after accumulating two more errors.
        captor1.getValue().run();

        Mockito.verify(log).error("Encountered {} {} within the last {}: {}", 2L, "errors", Duration.seconds(30), "Test error: third!", t3);
        ArgumentCaptor<Runnable> captor2 = ArgumentCaptor.forClass(Runnable.class);
        Mockito.verify(executor).schedule(captor2.capture(), Matchers.eq(30L), Matchers.eq(TimeUnit.SECONDS));
        Mockito.verifyNoMoreInteractions(log, executor);

        // Simulate the scheduled executor service running at the scheduled time, this time with no errors to report.
        captor2.getValue().run();

        Mockito.verifyNoMoreInteractions(log, executor);
    }

    @Test
    public void testDurationToString() {
        Assert.assertEquals("30 seconds", Duration.seconds(30).toString());
    }
}
