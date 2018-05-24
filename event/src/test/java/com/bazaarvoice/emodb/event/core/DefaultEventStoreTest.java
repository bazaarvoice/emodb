package com.bazaarvoice.emodb.event.core;

import com.bazaarvoice.emodb.event.api.EventStore;
import com.bazaarvoice.emodb.event.db.EventIdSerializer;
import com.bazaarvoice.emodb.event.db.EventReaderDAO;
import com.bazaarvoice.emodb.event.db.EventSink;
import com.bazaarvoice.emodb.event.db.EventWriterDAO;
import org.testng.annotations.Test;

import java.time.Duration;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DefaultEventStoreTest {

    /**
     * This tests the polling cache.
     * <p/>
     * If a subscription returns 0 events, cache this fact so that no repeated
     * calls are being made to cassandra for at least a second.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testBackingOffCache()
            throws InterruptedException {

        EventReaderDAO eventReaderDAO = mock(EventReaderDAO.class);
        EventWriterDAO eventWriterDAO = mock(EventWriterDAO.class);
        EventIdSerializer eventIdSerializer = mock(EventIdSerializer.class);
        ClaimStore claimStore = new MockClaimStore();

        EventStore eventStore = new DefaultEventStore(eventReaderDAO, eventWriterDAO, eventIdSerializer, claimStore);

        doNothing().when(eventReaderDAO).readNewer(anyString(), any(EventSink.class));

        eventStore.poll("channelA", Duration.ofSeconds(30), 50);

        verify(eventReaderDAO).readNewer(eq("channelA"), any(EventSink.class));

        // Now this subscription should be cached for a second
        eventStore.poll("channelA", Duration.ofSeconds(30), 50);

        // Verify we did not invoke eventReaderDAO the second time
        verify(eventReaderDAO, times(1)).readNewer(eq("channelA"), any(EventSink.class));

        // Wait 2 seconds to let the cache expire and verify that eventReaderDao is invoked
        Thread.sleep(2000);

        eventStore.poll("channelA", Duration.ofSeconds(30), 50);

        verify(eventReaderDAO, times(2)).readNewer(eq("channelA"), any(EventSink.class));
    }
}
