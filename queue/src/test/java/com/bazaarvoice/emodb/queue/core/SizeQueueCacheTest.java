package com.bazaarvoice.emodb.queue.core;

import com.bazaarvoice.emodb.event.api.BaseEventStore;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.job.api.JobType;
import com.bazaarvoice.emodb.queue.core.kafka.KafkaAdminService;
import com.bazaarvoice.emodb.queue.core.kafka.KafkaProducerService;
import com.bazaarvoice.emodb.queue.core.stepfn.StepFunctionService;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.testng.annotations.Test;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * This tests the queue size cache.
 * <p/>
 * A size call can be really expensive on cassandra servers for a large subscription.
 * Queue sizes are estimates, and take a 'limit' as an argument. The method counts upto
 * that limit, and then estimates the rest.
 * If an estimate with a higher 'limit' is already cached it returns what's in the cache.
 * If the limit required is higher than what's cached, the cache is invalidated.
 * The cache is expired after every 15 seconds.
 */
public class SizeQueueCacheTest {

    @Test
    public void testSizeCache() {

        final long start = System.currentTimeMillis();
        final Clock clock = mock(Clock.class);
        when(clock.millis()).thenReturn(start);

        BaseEventStore mockEventStore = mock(BaseEventStore.class);
        AbstractQueueService queueService = new AbstractQueueService(mockEventStore, mock(JobService.class),
                mock(JobHandlerRegistry.class), mock(JobType.class), clock, mock(KafkaAdminService.class), mock(KafkaProducerService.class),mock(StepFunctionService.class)){};

        // At limit=500, size estimate should be at 4800
        // At limit=50, size estimate should be at 5000
        when(mockEventStore.getSizeEstimate("testsubscription", 500L)).thenReturn(4800L);
        when(mockEventStore.getSizeEstimate("testsubscription", 50L)).thenReturn(5000L);

        // Let's get the size estimate with limit=50
        long size = queueService.getMessageCountUpTo("testsubscription", 50L);
        assertEquals(size, 5000L, "Size should be 5000");
        verify(mockEventStore, times(1)).getSizeEstimate("testsubscription", 50L);

        // verify no more interaction for the second call within 15 seconds
        size = queueService.getMessageCountUpTo("testsubscription", 50L);
        assertEquals(size, 5000L, "Size should be 5000");
        verifyNoMoreInteractions(mockEventStore);

        // verify that it does interact if the accuracy is increased limit=500
        size = queueService.getMessageCountUpTo("testsubscription", 500L);
        assertEquals(size, 4800L, "Size should be 4800");
        verify(mockEventStore, times(1)).getSizeEstimate("testsubscription", 500L);

        // verify that it does *not* interact if the accuracy is decreased limit=50 over the next 14 seconds
        for (int i=1; i <= 14; i++) {
            when(clock.millis()).thenReturn(start + TimeUnit.SECONDS.toMillis(i));
            size = queueService.getMessageCountUpTo("testsubscription", 50L);
            assertEquals(size, 4800L, "Size should still be 4800");
            verifyNoMoreInteractions(mockEventStore);
        }

        // Simulate one more second elapsed, making the total 15
        when(clock.millis()).thenReturn(start + TimeUnit.SECONDS.toMillis(15));

        size = queueService.getMessageCountUpTo("testsubscription", 50L);
        assertEquals(size, 5000L, "Size should be 5000");
        // By now it should've interacted twice in the entire testing cycle
        verify(mockEventStore, times(2)).getSizeEstimate("testsubscription", 50L);
    }

}
