package com.bazaarvoice.emodb.event.core;

import com.bazaarvoice.emodb.event.api.EventStore;
import com.bazaarvoice.emodb.event.api.SimpleEventSink;
import com.bazaarvoice.emodb.event.db.EventReaderDAO;
import com.bazaarvoice.emodb.event.db.EventSink;
import com.bazaarvoice.emodb.event.db.EventWriterDAO;
import com.bazaarvoice.emodb.event.db.astyanax.AstyanaxEventIdSerializer;
import com.bazaarvoice.emodb.event.dedup.DedupQueue;
import com.bazaarvoice.emodb.sortedq.api.SortedQueueFactory;
import com.bazaarvoice.emodb.sortedq.core.PersistentSortedQueue;
import com.bazaarvoice.emodb.sortedq.db.QueueDAO;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Suppliers;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class DedupQueueTest {
    @Test
    public void testPollSkipsEmptyChannels() {
        EventReaderDAO readerDao = mock(EventReaderDAO.class);
        EventStore eventStore = new DefaultEventStore(readerDao, mock(EventWriterDAO.class), new AstyanaxEventIdSerializer(), new MockClaimStore());

        DedupQueue q = new DedupQueue("test-queue", "read", "write",
                mock(QueueDAO.class), eventStore, Suppliers.ofInstance(true), mock(ScheduledExecutorService.class), getPersistentSortedQueueFactory(),
                mock(MetricRegistry.class));
        q.startAsync().awaitRunning();

        // The first poll checks the read channel, find it empty, checks the write channel.
        q.poll(Duration.ofSeconds(30), new SimpleEventSink(10));
        verify(readerDao).readNewer(eq("read"), Matchers.<EventSink>any());
        verify(readerDao).readNewer(eq("write"), Matchers.<EventSink>any());
        verifyNoMoreInteractions(readerDao);

        reset(readerDao);

        // Subsequent polls w/in a short window skips the poll operations.
        q.poll(Duration.ofSeconds(30), new SimpleEventSink(10));
        verifyNoMoreInteractions(readerDao);
    }

    @Test
    public void testPeekChecksAllChannels() {
        EventReaderDAO readerDao = mock(EventReaderDAO.class);
        EventStore eventStore = new DefaultEventStore(readerDao, mock(EventWriterDAO.class), new AstyanaxEventIdSerializer(), new MockClaimStore());

        DedupQueue q = new DedupQueue("test-queue", "read", "write",
                mock(QueueDAO.class), eventStore, Suppliers.ofInstance(true), mock(ScheduledExecutorService.class), getPersistentSortedQueueFactory(),
                mock(MetricRegistry.class));
        q.startAsync().awaitRunning();

        // The first peek checks the read channel, find it empty, checks the write channel.
        q.peek(new SimpleEventSink(10));
        verify(readerDao).readAll(eq("read"), Matchers.<EventSink>any(), (Date) Matchers.isNull(), Matchers.eq(true));
        verify(readerDao).readNewer(eq("write"), Matchers.<EventSink>any());
        verifyNoMoreInteractions(readerDao);

        reset(readerDao);

        // Subsequent peeks w/in a short window still peek the read channel, skip polling the write channel.
        q.peek(new SimpleEventSink(10));
        verify(readerDao).readAll(eq("read"), Matchers.<EventSink>any(), (Date) Matchers.isNull(), Matchers.eq(true));
        verifyNoMoreInteractions(readerDao);
    }

    private SortedQueueFactory getPersistentSortedQueueFactory() {
        SortedQueueFactory factory = mock(SortedQueueFactory.class);
        // use mockito to match any create() parameters and pass them on to the queue we instantiate
        Mockito.when(factory.create(Matchers.anyString(), Matchers.any(QueueDAO.class))).thenAnswer(new Answer() {
            public Object answer(InvocationOnMock invocation) {
                Object[] args = invocation.getArguments();
                return new PersistentSortedQueue((String)args[0], (QueueDAO)args[1], new MetricRegistry());
            }
        });

        return factory;
    }
}
