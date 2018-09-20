package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.databus.auth.ConstantDatabusAuthorizer;
import com.bazaarvoice.emodb.databus.auth.DatabusAuthorizer;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.kafka.KafkaConsumerConfiguration;
import com.bazaarvoice.emodb.databus.kafka.KafkaProducerConfiguration;
import com.bazaarvoice.emodb.event.api.DedupEventStore;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.event.api.EventSink;
import com.bazaarvoice.emodb.event.api.EventStore;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.eventbus.EventBus;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ConsolidationTest {
    /** EventStore.poll() returns 10 events, all for the same coordinate. */
    @Test
    public void testOneCoordinateManyEvents() {
        final List<String> actualIds = Lists.newArrayList();
        DatabusEventStore eventStore = new TestDatabusEventStore() {
            @Override
            public boolean poll(String subscription, Duration claimTtl, EventSink sink) {
                for (int i = 0; i < 10; i++) {
                    String id = "a" + i;
                    actualIds.add(id);
                    assertTrue(sink.remaining() > 0);
                    EventSink.Status status = sink.accept(newEvent(id, "table", "key", TimeUUIDs.newUUID()));
                    assertEquals(status, EventSink.Status.ACCEPTED_CONTINUE);
                }
                return true;
            }
        };
        Map<String, Object> content = entity("table", "key", ImmutableMap.of("rating", "5"));
        OwnerAwareDatabus databus = newDatabus(eventStore, new TestDataProvider().add(content));

        PollResult result = databus.poll("id", "test-subscription", Duration.ofSeconds(30), 1);
        List<Event> events = ImmutableList.copyOf(result.getEventIterator());

        Event first = events.get(0);
        assertEquals(first.getContent(), content);
        assertEquals(EventKeyFormat.decodeAll(Collections.singleton(first.getEventKey())), actualIds);
        assertEquals(events.size(), 1);
        assertTrue(result.hasMoreEvents());
    }

    /** EventStore.poll() returns 1000 events, all for the same coordinate. */
    @Test
    public void testOneCoordinateTooManyEvents() {
        final List<String> actualIds = Lists.newArrayList();
        DatabusEventStore eventStore = new TestDatabusEventStore() {
            @Override
            public boolean poll(String subscription, Duration claimTtl, EventSink sink) {
                // Loop exactly DefaultDatabus.MAX_EVENTS_TO_CONSOLIDATE times.  Verify the sink stops on the last.
                for (int i = 0; i < 1000; i++) {
                    String id = "a" + i;
                    actualIds.add(id);
                    assertTrue(sink.remaining() > 0);
                    EventSink.Status status = sink.accept(newEvent(id, "table", "key", TimeUUIDs.newUUID()));
                    assertEquals(status, (i < 999) ? EventSink.Status.ACCEPTED_CONTINUE : EventSink.Status.ACCEPTED_STOP);
                }
                return true;
            }
        };
        Map<String, Object> content = entity("table", "key", ImmutableMap.of("rating", "5"));
        OwnerAwareDatabus databus = newDatabus(eventStore, new TestDataProvider().add(content));

        PollResult result = databus.poll("id", "test-subscription", Duration.ofSeconds(30), 1);
        List<Event> events = ImmutableList.copyOf(result.getEventIterator());

        Event first = events.get(0);
        assertEquals(first.getContent(), content);
        assertEquals(EventKeyFormat.decodeAll(Collections.singleton(first.getEventKey())), Ordering.natural().immutableSortedCopy(actualIds));
        assertEquals(events.size(), 1);
        assertTrue(result.hasMoreEvents());
    }

    @Test
    public void testTwoCoordinatesManyEvents() {
        final List<String> actualIds = Lists.newArrayList();
        DatabusEventStore eventStore = new TestDatabusEventStore() {
            @Override
            public boolean poll(String subscription, Duration claimTtl, EventSink sink) {
                for (int i = 0; i < 10; i++) {
                    String id = "a" + i;
                    actualIds.add(id);
                    assertTrue(sink.remaining() > 0);
                    EventSink.Status status = sink.accept(newEvent(id, "table", "key", TimeUUIDs.newUUID()));
                    assertEquals(status, EventSink.Status.ACCEPTED_CONTINUE);
                }
                // Verify that the Databus stops as soon as it receives an event for a different coordinate.
                assertTrue(sink.remaining() > 0);
                EventSink.Status status = sink.accept(newEvent("b0", "table", "key2", TimeUUIDs.newUUID()));
                assertEquals(status, EventSink.Status.REJECTED_STOP);
                return true;
            }
        };
        Map<String, Object> content = entity("table", "key", ImmutableMap.of("rating", "5"));
        OwnerAwareDatabus databus = newDatabus(eventStore, new TestDataProvider().add(content));

        PollResult result = databus.poll("id", "test-subscription", Duration.ofSeconds(30), 1);
        List<Event> events = ImmutableList.copyOf(result.getEventIterator());

        Event first = events.get(0);
        assertEquals(first.getContent(), content);
        assertEquals(EventKeyFormat.decodeAll(Collections.singleton(first.getEventKey())), actualIds);
        assertEquals(events.size(), 1);
        assertTrue(result.hasMoreEvents());
    }

    @Test
    public void testOneCoordinateManyTags() {
        final List<String> actualIds = Lists.newArrayList();
        DatabusEventStore eventStore = new TestDatabusEventStore() {
            @Override
            public boolean poll(String subscription, Duration claimTtl, EventSink sink) {
                for (int i = 0; i < 8; i++) {
                    String id = "a" + i;
                    actualIds.add(id);
                    assertTrue(sink.remaining() > 0);
                    Set<String> tags;
                    switch (i % 4) {
                        case 0:
                            tags = ImmutableSet.of("tag2", "tag3");
                            break;
                        case 1:
                            tags = ImmutableSet.of("tag1", "tag2");
                            break;
                        case 2:
                            tags = ImmutableSet.of();
                            break;
                        default:
                            tags = ImmutableSet.of("tag2", "tag3", "tag4");
                            break;
                    }
                    EventSink.Status status = sink.accept(newEvent(id, "table", "key", TimeUUIDs.newUUID(), tags));
                    assertEquals(status, EventSink.Status.ACCEPTED_CONTINUE);
                }
                return true;
            }
        };
        Map<String, Object> content = entity("table", "key", ImmutableMap.of("rating", "5"));
        OwnerAwareDatabus databus = newDatabus(eventStore, new TestDataProvider().add(content));

        PollResult result = databus.poll("id", "test-subscription", Duration.ofSeconds(30), 1);
        List<Event> events = ImmutableList.copyOf(result.getEventIterator());

        Event first = events.get(0);
        assertEquals(first.getContent(), content);
        assertEquals(EventKeyFormat.decodeAll(Collections.singleton(first.getEventKey())), actualIds);
        assertEquals(first.getTags(), ImmutableList.builder()
                .add(ImmutableList.<String>of())
                .add(ImmutableList.of("tag1", "tag2"))
                .add(ImmutableList.of("tag2", "tag3"))
                .add(ImmutableList.of("tag2", "tag3", "tag4"))
                .build());
        assertEquals(events.size(), 1);
        assertTrue(result.hasMoreEvents());
    }

    @Test
    public void testOneCoordinateManyTagsMultipleEventStorePolls() {
        final List<String> actualIds = Lists.newArrayList();
        DatabusEventStore eventStore = new TestDatabusEventStore() {
            private int iteration = 0;

            @Override
            public boolean poll(String subscription, Duration claimTtl, EventSink sink) {
                if (iteration++ < 8) {
                    String id = "a" + iteration;
                    actualIds.add(id);
                    assertTrue(sink.remaining() > 0);
                    Set<String> tags;
                    switch (iteration % 4) {
                        case 0:
                            tags = ImmutableSet.of("tag2", "tag3");
                            break;
                        case 1:
                            tags = ImmutableSet.of("tag1", "tag2");
                            break;
                        case 2:
                            tags = ImmutableSet.of();
                            break;
                        default:
                            tags = ImmutableSet.of("tag2", "tag3", "tag4");
                            break;
                    }
                    EventSink.Status status = sink.accept(newEvent(id, "table", "key", TimeUUIDs.newUUID(), tags));
                    assertEquals(status, EventSink.Status.ACCEPTED_CONTINUE);
                    return true;
                }
                return false;
            }
        };
        Map<String, Object> content = entity("table", "key", ImmutableMap.of("rating", "5"));

        // Use a custom ticker to force exiting the loop because there is no more data and not because of a timeout
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        DefaultDatabus databus = newDatabus(eventStore, new TestDataProvider().add(content), clock);

        // Use a limit of 2 to force multiple calls to the event store.
        PollResult result = databus.poll("id", "test-subscription", Duration.ofSeconds(30), 2);
        List<Event> events = ImmutableList.copyOf(result.getEventIterator());

        Event first = events.get(0);
        assertEquals(first.getContent(), content);
        assertEquals(EventKeyFormat.decodeAll(Collections.singleton(first.getEventKey())), actualIds);
        assertEquals(first.getTags(), ImmutableList.builder()
                .add(ImmutableList.<String>of())
                .add(ImmutableList.of("tag1", "tag2"))
                .add(ImmutableList.of("tag2", "tag3"))
                .add(ImmutableList.of("tag2", "tag3", "tag4"))
                .build());
        assertEquals(events.size(), 1);
        assertFalse(result.hasMoreEvents());
    }

    @Test
    public void testDiscardedUpdates() {
        final List<String> actualIds = Lists.newArrayList();
        DedupEventStore dedupEventStore = mock(DedupEventStore.class);
        DatabusEventStore eventStore = new DatabusEventStore(mock(EventStore.class), dedupEventStore, Suppliers.ofInstance(true)) {
            @Override
            public boolean poll(String subscription, Duration claimTtl, EventSink sink) {
                // The single poll will supply 10 redundant events followed by an empty queue return value
                for (int i=0; i < 10; i++) {
                    String id = "a" + i;
                    actualIds.add(id);
                    assertTrue(sink.remaining() > 0);
                    EventSink.Status status = sink.accept(newEvent(id, "table", "key", TimeUUIDs.newUUID()));
                    assertEquals(status, EventSink.Status.ACCEPTED_CONTINUE);
                }
                return false;
            }
        };
        Map<String, Object> content = entity("table", "key", ImmutableMap.of("rating", "5"));
        // Create a custom annotated content which returns all changes as redundant
        DataProvider.AnnotatedContent annotatedContent = mock(DataProvider.AnnotatedContent.class);
        when(annotatedContent.getContent()).thenReturn(content);
        when(annotatedContent.isChangeDeltaRedundant(any(UUID.class))).thenReturn(true);
        OwnerAwareDatabus databus = newDatabus(eventStore, new TestDataProvider().add(annotatedContent));

        PollResult result = databus.poll("id", "test-subscription", Duration.ofSeconds(30), 2);
        assertFalse(result.getEventIterator().hasNext());
        assertFalse(result.hasMoreEvents());

        // Since all events came from the same batch from the underlying event store they should all be deleted in one call.
        verify(dedupEventStore).delete("test-subscription", actualIds, true);
        verifyNoMoreInteractions(dedupEventStore);
    }

    @Test
    public void testDiscardedUpdatesOverMultipleEventStorePolls() {
        final List<String> actualIds = Lists.newArrayList();
        DedupEventStore dedupEventStore = mock(DedupEventStore.class);
        DatabusEventStore eventStore = new DatabusEventStore(mock(EventStore.class), dedupEventStore, Suppliers.ofInstance(true)) {
            int iteration = 0;

            @Override
            public boolean poll(String subscription, Duration claimTtl, EventSink sink) {
                // The first 10 polls will supply a single redundant update each, then the 11th poll will return
                // an empty queue return value
                if (iteration++ < 10) {
                    String id = "a" + iteration;
                    actualIds.add(id);
                    assertTrue(sink.remaining() > 0);
                    EventSink.Status status = sink.accept(newEvent(id, "table", "key", TimeUUIDs.newUUID()));
                    assertEquals(status, EventSink.Status.ACCEPTED_CONTINUE);
                    return true;
                }
                return false;
            }
        };
        Map<String, Object> content = entity("table", "key", ImmutableMap.of("rating", "5"));
        // Create a custom annotated content which returns all changes as redundant
        DataProvider.AnnotatedContent annotatedContent = mock(DataProvider.AnnotatedContent.class);
        when(annotatedContent.getContent()).thenReturn(content);
        when(annotatedContent.isChangeDeltaRedundant(any(UUID.class))).thenReturn(true);
        OwnerAwareDatabus databus = newDatabus(eventStore, new TestDataProvider().add(annotatedContent));

        PollResult result = databus.poll("id", "test-subscription", Duration.ofSeconds(30), 1);
        assertFalse(result.getEventIterator().hasNext());
        assertFalse(result.hasMoreEvents());

        // Since each event came from a separate batch from the underlying event store they should each be deleted
        // in separate calls.
        for (String actualId : actualIds) {
            verify(dedupEventStore).delete("test-subscription", ImmutableList.of(actualId), true);
        }
        verifyNoMoreInteractions(dedupEventStore);
    }

    private DefaultDatabus newDatabus(DatabusEventStore eventStore, DataProvider dataProvider) {
        return newDatabus(eventStore, dataProvider, Clock.systemUTC());
    }

    private DefaultDatabus newDatabus(DatabusEventStore eventStore, DataProvider dataProvider, Clock clock) {
        LifeCycleRegistry lifeCycle = mock(LifeCycleRegistry.class);
        EventBus eventBus = mock(EventBus.class);
        SubscriptionDAO subscriptionDao = mock(SubscriptionDAO.class);
        SubscriptionEvaluator subscriptionEvaluator = mock(SubscriptionEvaluator.class);
        JobService jobService = mock(JobService.class);
        JobHandlerRegistry jobHandlerRegistry = mock(JobHandlerRegistry.class);
        DatabusAuthorizer databusAuthorizer = ConstantDatabusAuthorizer.ALLOW_ALL;
        return new DefaultDatabus(lifeCycle, eventBus, dataProvider, subscriptionDao, eventStore, subscriptionEvaluator,
                jobService, jobHandlerRegistry, databusAuthorizer, "replication",
                Suppliers.ofInstance(Conditions.alwaysFalse()), mock(ExecutorService.class),
                new KafkaProducerConfiguration(), new KafkaProducerConfiguration(), new Boolean(false), new Boolean(false), new Boolean(false), 1, key -> 0,
                new MetricRegistry(), clock);
    }

    private static EventData newEvent(final String id, String table, String key, UUID changeId) {
        return newEvent(id, table, key, changeId, ImmutableSet.<String>of());
    }

    private static EventData newEvent(final String id, String table, String key, UUID changeId, Set<String> tags) {
        final ByteBuffer data = UpdateRefSerializer.toByteBuffer(new UpdateRef(table, key, changeId, tags));
        return new EventData() {
            @Override
            public String getId() {
                return id;
            }

            @Override
            public ByteBuffer getData() {
                return data;
            }
        };
    }

    private static Map<String, Object> entity(String table, String key, Map<String, ?> data) {
        return ImmutableMap.<String, Object>builder()
                .put(Intrinsic.VERSION, 1)
                .putAll(data)
                .putAll(Coordinate.of(table, key).asJson())
                .build();
    }
}
