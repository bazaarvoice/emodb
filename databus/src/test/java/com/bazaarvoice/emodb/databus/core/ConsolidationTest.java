package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.auth.ConstantDatabusAuthorizer;
import com.bazaarvoice.emodb.databus.auth.DatabusAuthorizer;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.event.api.EventSink;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.eventbus.EventBus;
import org.joda.time.Duration;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
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

        List<Event> events = databus.poll("id", "test-subscription", Duration.standardSeconds(30), 1);

        Event first = events.get(0);
        assertEquals(first.getContent(), content);
        assertEquals(EventKeyFormat.decodeAll(Collections.singleton(first.getEventKey())), actualIds);
        assertEquals(events.size(), 1);
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

        List<Event> events = databus.poll("id", "test-subscription", Duration.standardSeconds(30), 1);

        Event first = events.get(0);
        assertEquals(first.getContent(), content);
        assertEquals(EventKeyFormat.decodeAll(Collections.singleton(first.getEventKey())), Ordering.natural().immutableSortedCopy(actualIds));
        assertEquals(events.size(), 1);
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

        List<Event> events = databus.poll("id", "test-subscription", Duration.standardSeconds(30), 1);

        Event first = events.get(0);
        assertEquals(first.getContent(), content);
        assertEquals(EventKeyFormat.decodeAll(Collections.singleton(first.getEventKey())), actualIds);
        assertEquals(events.size(), 1);
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
                        case 0:  tags = ImmutableSet.of("tag2", "tag3");  break;
                        case 1:  tags = ImmutableSet.of("tag1", "tag2");  break;
                        case 2:  tags = ImmutableSet.of();  break;
                        default: tags = ImmutableSet.of("tag2", "tag3", "tag4");  break;
                    }
                    EventSink.Status status = sink.accept(newEvent(id, "table", "key", TimeUUIDs.newUUID(), tags));
                    assertEquals(status, EventSink.Status.ACCEPTED_CONTINUE);
                }
                return true;
            }
        };
        Map<String, Object> content = entity("table", "key", ImmutableMap.of("rating", "5"));
        OwnerAwareDatabus databus = newDatabus(eventStore, new TestDataProvider().add(content));

        List<Event> events = databus.poll("id", "test-subscription", Duration.standardSeconds(30), 1);

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
                        case 0:  tags = ImmutableSet.of("tag2", "tag3");  break;
                        case 1:  tags = ImmutableSet.of("tag1", "tag2");  break;
                        case 2:  tags = ImmutableSet.of();  break;
                        default: tags = ImmutableSet.of("tag2", "tag3", "tag4");  break;
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
        final long now = Ticker.systemTicker().read();
        Ticker ticker = new Ticker() {
            @Override
            public long read() {
                return now;  // Current time never changes
            }
        };

        DefaultDatabus databus = newDatabus(eventStore, new TestDataProvider().add(content), ticker);

        // Use a limit of 2 to force multiple calls to the event store.
        List<Event> events = databus.poll("id", "test-subscription", Duration.standardSeconds(30), 2);

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
    }

    private DefaultDatabus newDatabus(DatabusEventStore eventStore, DataProvider dataProvider) {
        return newDatabus(eventStore, dataProvider, Ticker.systemTicker());
    }

    private DefaultDatabus newDatabus(DatabusEventStore eventStore, DataProvider dataProvider, Ticker ticker) {
        LifeCycleRegistry lifeCycle = mock(LifeCycleRegistry.class);
        EventBus eventBus = mock(EventBus.class);
        SubscriptionDAO subscriptionDao = mock(SubscriptionDAO.class);
        SubscriptionEvaluator subscriptionEvaluator = mock(SubscriptionEvaluator.class);
        JobService jobService = mock(JobService.class);
        JobHandlerRegistry jobHandlerRegistry = mock(JobHandlerRegistry.class);
        DatabusAuthorizer databusAuthorizer = ConstantDatabusAuthorizer.ALLOW_ALL;
        return new DefaultDatabus(lifeCycle, eventBus, dataProvider, subscriptionDao, eventStore, subscriptionEvaluator,
                jobService, jobHandlerRegistry, databusAuthorizer, "replication", new MetricRegistry(), ticker);
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
