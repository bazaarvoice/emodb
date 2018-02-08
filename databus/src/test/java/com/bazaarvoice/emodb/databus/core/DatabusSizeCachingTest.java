package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.databus.auth.DatabusAuthorizer;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Suppliers;
import com.google.common.eventbus.EventBus;
import org.testng.annotations.Test;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class DatabusSizeCachingTest {

    /**
     * This tests the databus size cache.
     * <p/>
     * A size call can be really expensive on cassandra servers for a large subscription.
     * Queue sizes are estimates, and take a 'limit' as an argument. The method counts upto
     * that limit, and then estimates the rest.
     * If an estimate with a higher 'limit' is already cached it returns what's in the cache.
     * If the limit required is higher than what's cached, the cache is invalidated.
     * The cache is expired after every 15 seconds.
     */
    @Test
    @SuppressWarnings ("unchecked")
    public void testSizeCache() {
        final Clock clock = mock(Clock.class);
        long start = System.currentTimeMillis();
        when(clock.millis()).thenReturn(start);

        DatabusEventStore mockEventStore = mock(DatabusEventStore.class);
        DefaultDatabus testDatabus = new DefaultDatabus(
                mock(LifeCycleRegistry.class), mock(EventBus.class), mock(DataProvider.class), mock(SubscriptionDAO.class),
                mockEventStore, mock(SubscriptionEvaluator.class), mock(JobService.class), mock(JobHandlerRegistry.class),
                mock(DatabusAuthorizer.class), "replication", Suppliers.ofInstance(Conditions.alwaysFalse()), mock(ExecutorService.class),
                1, key -> 0, mock(MetricRegistry.class), clock);

        // At limit=500, size estimate should be at 4800
        // At limit=50, size estimate should be at 5000
        when(mockEventStore.getSizeEstimate("testsubscription", 500L)).thenReturn(4800L);
        when(mockEventStore.getSizeEstimate("testsubscription", 50L)).thenReturn(5000L);

        // Let's get the size estimate with limit=50
        long size = testDatabus.getEventCountUpTo("id", "testsubscription", 50L);
        assertEquals(size, 5000L, "Size should be 5000");
        verify(mockEventStore, times(1)).getSizeEstimate("testsubscription", 50L);

        // verify no more interaction for the second call within 15 seconds
        size = testDatabus.getEventCountUpTo("id", "testsubscription", 50L);
        assertEquals(size, 5000L, "Size should be 5000");
        verifyNoMoreInteractions(mockEventStore);

        // verify that it does interact if the accuracy is increased limit=500
        size = testDatabus.getEventCountUpTo("id", "testsubscription", 500L);
        assertEquals(size, 4800L, "Size should be 4800");
        verify(mockEventStore, times(1)).getSizeEstimate("testsubscription", 500L);

        // verify that it does *not* interact if the accuracy is decreased limit=50 over the next 14 seconds
        for (int i = 1; i <= 14; i++) {
            when(clock.millis()).thenReturn(start + TimeUnit.SECONDS.toMillis(i));
            size = testDatabus.getEventCountUpTo("id", "testsubscription", 50L);
            assertEquals(size, 4800L, "Size should still be 4800");
            verifyNoMoreInteractions(mockEventStore);
        }

        // Simulate one more second elapsed, making the total 15
        when(clock.millis()).thenReturn(start + TimeUnit.SECONDS.toMillis(15));

        size = testDatabus.getEventCountUpTo("id", "testsubscription", 50L);
        assertEquals(size, 5000L, "Size should be 5000");
        // By now it should've interacted twice in the entire testing cycle
        verify(mockEventStore, times(2)).getSizeEstimate("testsubscription", 50L);
    }
}
