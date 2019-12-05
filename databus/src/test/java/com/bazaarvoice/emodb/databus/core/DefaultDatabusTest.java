package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.databus.auth.ConstantDatabusAuthorizer;
import com.bazaarvoice.emodb.databus.auth.DatabusAuthorizer;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.DefaultOwnedSubscription;
import com.bazaarvoice.emodb.event.api.DedupEventStore;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.event.api.EventSink;
import com.bazaarvoice.emodb.event.api.EventStore;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.core.DatabusEventWriterRegistry;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.beust.jcommander.internal.Sets;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.MoreExecutors;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DefaultDatabusTest {
    @Test
    public void testSubscriptionCreation() {
        // Currently, by default, includeDefaultJoinFilter is set to true,
        // which means that we append the skipIgnored tags condition to the original table filter
        // unless the caller explicitly sets the includeDefaultJoinFilter to false.
        // This is just an interim setup to be backwards compatible. Soon we will deprecate the
        // includeDefaultJoinFilter flag.

        Supplier<Condition> ignoreReEtl = Suppliers.ofInstance(
                Conditions.not(Conditions.mapBuilder().matches(UpdateRef.TAGS_NAME, Conditions.containsAny("re-etl")).build()));
        SubscriptionDAO mockSubscriptionDao = mock(SubscriptionDAO.class);
        DefaultDatabus testDatabus = new DefaultDatabus(
                mock(LifeCycleRegistry.class), mock(DatabusEventWriterRegistry.class), mock(DataProvider.class), mockSubscriptionDao,
                mock(DatabusEventStore.class), mock(SubscriptionEvaluator.class), mock(JobService.class),
                mock(JobHandlerRegistry.class), mock(DatabusAuthorizer.class), "replication", ignoreReEtl, mock(ExecutorService.class),
                1, key -> 0, mock(MetricRegistry.class), Clock.systemUTC());
        Condition originalCondition = Conditions.mapBuilder().contains("foo", "bar").build();
        testDatabus.subscribe("id", "test-subscription", originalCondition, Duration.ofDays(7),
                Duration.ofDays(7));
        // Skip databus events tagged with "re-etl"
        Condition skipIgnoreTags = Conditions.not(Conditions.mapBuilder().matches(UpdateRef.TAGS_NAME, Conditions.containsAny("re-etl")).build());
        Condition expectedConditionToSkipIgnore = Conditions.and(originalCondition, skipIgnoreTags);
        verify(mockSubscriptionDao).insertSubscription("id", "test-subscription", expectedConditionToSkipIgnore,
                Duration.ofDays(7), Duration.ofDays(7));
        verify(mockSubscriptionDao).getSubscription("test-subscription");
        verifyNoMoreInteractions(mockSubscriptionDao);

        // reset mocked subscription DAO so it doesn't carry information about old interactions
        reset(mockSubscriptionDao);
        // Test condition is unchanged if includeDefaultJoinFilter is set to false
        testDatabus.subscribe("id", "test-subscription", originalCondition, Duration.ofDays(7),
                Duration.ofDays(7), false);
        verify(mockSubscriptionDao).insertSubscription("id", "test-subscription", originalCondition, Duration.ofDays(7),
                Duration.ofDays(7));
        verify(mockSubscriptionDao).getSubscription("test-subscription");
        verifyNoMoreInteractions(mockSubscriptionDao);
    }

    @Test
    public void testDrainQueueForAllNonRedundantItemsInOnePeek() {

        Supplier<Condition> ignoreReEtl = Suppliers.ofInstance(
                Conditions.not(Conditions.mapBuilder().matches(UpdateRef.TAGS_NAME, Conditions.containsAny("re-etl")).build()));
        final List<String> actualIds = Lists.newArrayList();
        DedupEventStore dedupEventStore = mock(DedupEventStore.class);
        DatabusEventStore eventStore = new DatabusEventStore(mock(EventStore.class), dedupEventStore, Suppliers.ofInstance(true)) {
            @Override
            public boolean peek(String subscription, EventSink sink) {
                // The single peek will supply 3 redundant events followed by an empty queue return value
                for (int i = 0; i < 3; i++) {
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
        // Create a custom annotated content which returns all changes as not redundant
        DataProvider.AnnotatedContent annotatedContent = mock(DataProvider.AnnotatedContent.class);
        when(annotatedContent.getContent()).thenReturn(content);
        when(annotatedContent.isChangeDeltaRedundant(any(UUID.class))).thenReturn(false); // Items are not redundant.

        DefaultDatabus testDatabus = new DefaultDatabus(
                mock(LifeCycleRegistry.class), mock(DatabusEventWriterRegistry.class), new TestDataProvider().add(annotatedContent), mock(SubscriptionDAO.class),
                eventStore, mock(SubscriptionEvaluator.class), mock(JobService.class),
                mock(JobHandlerRegistry.class), mock(DatabusAuthorizer.class), "systemOwnerId", ignoreReEtl, MoreExecutors.newDirectExecutorService(),
                1, key -> 0, new MetricRegistry(), Clock.systemUTC());

        // Call the drainQueue method.
        testDatabus.drainQueueAsync("test-subscription");

        // no deletes should be happening.
        verifyZeroInteractions(dedupEventStore);

        // the entry should be removed from map.
        assertEquals(testDatabus.getDrainedSubscriptionsMap().size(), 0);
    }

    @Test
    public void testDrainQueueForAllRedundantItemsInOnePeek() {

        Supplier<Condition> ignoreReEtl = Suppliers.ofInstance(
                Conditions.not(Conditions.mapBuilder().matches(UpdateRef.TAGS_NAME, Conditions.containsAny("re-etl")).build()));
        final List<String> actualIds = Lists.newArrayList();
        DedupEventStore dedupEventStore = mock(DedupEventStore.class);
        DatabusEventStore eventStore = new DatabusEventStore(mock(EventStore.class), dedupEventStore, Suppliers.ofInstance(true)) {
            @Override
            public boolean peek(String subscription, EventSink sink) {
                // The single peek will supply 3 redundant events followed by an empty queue return value
                for (int i = 0; i < 3; i++) {
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
        when(annotatedContent.isChangeDeltaRedundant(any(UUID.class))).thenReturn(true); // Items are redundant.

        DefaultDatabus testDatabus = new DefaultDatabus(
                mock(LifeCycleRegistry.class), mock(DatabusEventWriterRegistry.class), new TestDataProvider().add(annotatedContent), mock(SubscriptionDAO.class),
                eventStore, mock(SubscriptionEvaluator.class), mock(JobService.class),
                mock(JobHandlerRegistry.class), mock(DatabusAuthorizer.class), "systemOwnerId", ignoreReEtl, MoreExecutors.newDirectExecutorService(),
                1, key -> 0, new MetricRegistry(), Clock.systemUTC());

        // Call the drainQueue method.
        testDatabus.drainQueueAsync("test-subscription");

        // deletes should happen.
        verify(dedupEventStore).delete("test-subscription", actualIds, true);
        verifyNoMoreInteractions(dedupEventStore);

        // the entry should be removed from map.
        assertEquals(testDatabus.getDrainedSubscriptionsMap().size(), 0);
    }

    @Test
    public void testDrainQueueForItemsInMultiplePeeks() {
        Supplier<Condition> ignoreReEtl = Suppliers.ofInstance(
                Conditions.not(Conditions.mapBuilder().matches(UpdateRef.TAGS_NAME, Conditions.containsAny("re-etl")).build()));
        final List<String> actualIds = Lists.newArrayList();
        DedupEventStore dedupEventStore = mock(DedupEventStore.class);
        DatabusEventStore eventStore = new DatabusEventStore(mock(EventStore.class), dedupEventStore, Suppliers.ofInstance(true)) {
            private int iteration = 0;

            @Override
            public boolean peek(String subscription, EventSink sink) {
                // The single peek will return one item followed with a "more":true value. Finally, a "more":false value when done.
                if (iteration++ < 3) {
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

        DefaultDatabus testDatabus = new DefaultDatabus(
                mock(LifeCycleRegistry.class), mock(DatabusEventWriterRegistry.class), new TestDataProvider().add(annotatedContent), mock(SubscriptionDAO.class),
                eventStore, mock(SubscriptionEvaluator.class), mock(JobService.class),
                mock(JobHandlerRegistry.class), mock(DatabusAuthorizer.class), "systemOwnerId", ignoreReEtl, MoreExecutors.newDirectExecutorService(),
                1, key -> 0, new MetricRegistry(), Clock.systemUTC());

        // Call the drainQueue method.
        testDatabus.drainQueueAsync("test-subscription");

        assertEquals(testDatabus.getDrainedSubscriptionsMap().size(), 0);

        for (String actualId : actualIds) {
            verify(dedupEventStore).delete("test-subscription", ImmutableList.of(actualId), true);
        }
        verifyNoMoreInteractions(dedupEventStore);

        // the entry should be removed from map.
        assertEquals(testDatabus.getDrainedSubscriptionsMap().size(), 0);
    }

    @Test
    public void testLazyPollResult() {
        Supplier<Condition> acceptAll = Suppliers.ofInstance(Conditions.alwaysTrue());
        TestDataProvider testDataProvider = new TestDataProvider();

        final Set<String> expectedIds = Sets.newHashSet();

        DatabusEventStore eventStore = mock(DatabusEventStore.class);
        when(eventStore.poll(eq("subscription"), eq(Duration.ofMinutes(1)), any(EventSink.class)))
                .thenAnswer(invocationOnMock -> {
                    EventSink sink = (EventSink) invocationOnMock.getArguments()[2];
                    // Return 40 updates for records from 40 unique tables
                    for (int iteration = 1; iteration <= 40; iteration++) {
                        String id = "a" + iteration;
                        addToPoll(id, "table-" + iteration, "key-" + iteration, false, sink, testDataProvider);
                        expectedIds.add(id);
                    }
                    return false;
                });
        SubscriptionDAO subscriptionDAO = mock(SubscriptionDAO.class);
        when(subscriptionDAO.getSubscription("subscription")).thenReturn(
                new DefaultOwnedSubscription("subscription", Conditions.alwaysTrue(), new Date(1489090060000L),
                        Duration.ofSeconds(30), "owner"));

        DatabusAuthorizer databusAuthorizer = ConstantDatabusAuthorizer.ALLOW_ALL;

        // Use a clock that advances 1 second after the first call
        Clock clock = mock(Clock.class);
        when(clock.millis())
                .thenReturn(1489090000000L)
                .thenReturn(1489090001000L);

        DefaultDatabus testDatabus = new DefaultDatabus(
                mock(LifeCycleRegistry.class), mock(DatabusEventWriterRegistry.class), testDataProvider, subscriptionDAO,
                eventStore, mock(SubscriptionEvaluator.class), mock(JobService.class),
                mock(JobHandlerRegistry.class), databusAuthorizer, "systemOwnerId", acceptAll, MoreExecutors.newDirectExecutorService(),
                1, key -> 0, new MetricRegistry(), clock);

        PollResult pollResult = testDatabus.poll("owner", "subscription", Duration.ofMinutes(1), 500);
        assertFalse(pollResult.hasMoreEvents());

        Iterator<Event> events = pollResult.getEventIterator();
        Set<String> actualIds = Sets.newHashSet();
        // Read the entire event list
        while (events.hasNext()) {
            actualIds.add(events.next().getEventKey());
        }
        assertEquals(actualIds, expectedIds);
        // Events should have been loaded in 3 batches.  The first was a synchronous batch that loaded the first 10 events.
        // The seconds should have loaded 25 more events.  The third loaded the final 5 events;
        List<List<Coordinate>> executions = testDataProvider.getExecutions();
        assertEquals(executions.size(), 3);
        assertEquals(executions.get(0).size(), 10);
        assertEquals(executions.get(1).size(), 25);
        assertEquals(executions.get(2).size(), 5);
    }

    @Test
    public void testLazyPollResultWithPaddedEvents() {
        Supplier<Condition> acceptAll = Suppliers.ofInstance(Conditions.alwaysTrue());
        TestDataProvider testDataProvider = new TestDataProvider();

        final Set<String> expectedPollIds = Sets.newHashSet();
        final Set<String> expectedDeleteIds = Sets.newHashSet();
        final Set<String> expectedUnclaimIds = Sets.newHashSet();

        DatabusEventStore eventStore = mock(DatabusEventStore.class);
        when(eventStore.poll(eq("subscription"), eq(Duration.ofMinutes(1)), any(EventSink.class)))
                .thenAnswer(invocationOnMock -> {
                    // For the first poll, return 10 events which will all be redundant
                    EventSink sink = (EventSink) invocationOnMock.getArguments()[2];
                    for (int iteration = 1; iteration <= 10; iteration++) {
                        String id = "a" + iteration;
                        addToPoll(id, "test-" + iteration, "key-" + iteration, true, sink, testDataProvider);
                        expectedDeleteIds.add(id);
                    }
                    return true;
                })
                .thenAnswer(invocationOnMock -> {
                    // For the second poll, return 20 events which are all not redundant, though only the first 10
                    // of which should be returned with a poll limit of 10.  The remaining 10 are padding.
                    EventSink sink = (EventSink) invocationOnMock.getArguments()[2];
                    for (int iteration = 11; iteration <= 20; iteration++) {
                        String id = "a" + iteration;
                        addToPoll(id, "test-" + iteration, "key-" + iteration, false, sink, testDataProvider);
                        expectedPollIds.add(id);
                    }
                    for (int iteration = 21; iteration <= 30; iteration++) {
                        String id = "a" + iteration;
                        addToPoll(id, "test-" + iteration, "key-" + iteration, false, sink, testDataProvider);
                        expectedUnclaimIds.add(id);
                    }
                    return false;
                });

        SubscriptionDAO subscriptionDAO = mock(SubscriptionDAO.class);
        when(subscriptionDAO.getSubscription("subscription")).thenReturn(
                new DefaultOwnedSubscription("subscription", Conditions.alwaysTrue(), new Date(1489090060000L),
                        Duration.ofSeconds(30), "owner"));

        DatabusAuthorizer databusAuthorizer = ConstantDatabusAuthorizer.ALLOW_ALL;
        Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

        DefaultDatabus testDatabus = new DefaultDatabus(
                mock(LifeCycleRegistry.class), mock(DatabusEventWriterRegistry.class), testDataProvider, subscriptionDAO,
                eventStore, mock(SubscriptionEvaluator.class), mock(JobService.class),
                mock(JobHandlerRegistry.class), databusAuthorizer, "systemOwnerId", acceptAll, MoreExecutors.newDirectExecutorService(),
                1, key -> 0, new MetricRegistry(), clock);

        PollResult pollResult = testDatabus.poll("owner", "subscription", Duration.ofMinutes(1), 10);
        // Because of padding all events were read from the event store.  However, since the padded events will be
        // unclaimed the result should return that there are more events.
        assertTrue(pollResult.hasMoreEvents());

        // Verify that the redundant events where deleted
        verify(eventStore).delete(eq("subscription"), argThat(containsExactly(expectedDeleteIds)), eq(true));
        // Padded events will be unclaimed lazily upon iterating over the event list, so verify they haven't been unclaimed yet
        verify(eventStore, never()).renew(anyString(), anyCollectionOf(String.class), any(Duration.class), anyBoolean());

        Iterator<Event> events = pollResult.getEventIterator();
        // Read the entire event list
        Set<String> actualIds = Sets.newHashSet();
        while (events.hasNext()) {
            actualIds.add(events.next().getEventKey());
        }

        assertEquals(actualIds, expectedPollIds);

        // Verify the padded events were unclaimed
        verify(eventStore).renew(eq("subscription"), argThat(containsExactly(expectedUnclaimIds)), eq(Duration.ZERO), eq(false));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFanoutToMasterPartitions() {
        Supplier<Condition> acceptAll = Suppliers.ofInstance(Conditions.alwaysTrue());

        final SetMultimap<String, UpdateRef> eventsStored = HashMultimap.create();
        DatabusEventStore eventStore = mock(DatabusEventStore.class);
        doAnswer(invocationOnMock -> {
            synchronized (eventsStored) {
                Multimap<String, ByteBuffer> rawEvents = (Multimap<String, ByteBuffer>) invocationOnMock.getArguments()[0];
                for (Map.Entry<String, ByteBuffer> entry : rawEvents.entries()) {
                    eventsStored.put(entry.getKey(), UpdateRefSerializer.fromByteBuffer(entry.getValue()));
                }
            }
            return null;
        }).when(eventStore).addAll(any(Multimap.class));

        PartitionSelector masterPartitioner = mock(PartitionSelector.class);
        when(masterPartitioner.getPartition("key0")).thenReturn(0);
        when(masterPartitioner.getPartition("key1")).thenReturn(1);
        when(masterPartitioner.getPartition("key2")).thenReturn(2);
        when(masterPartitioner.getPartition("key3")).thenReturn(0);

        DefaultDatabus testDatabus = new DefaultDatabus(
                mock(LifeCycleRegistry.class), mock(DatabusEventWriterRegistry.class), new TestDataProvider(), mock(SubscriptionDAO.class),
                eventStore, mock(SubscriptionEvaluator.class), mock(JobService.class),
                mock(JobHandlerRegistry.class), mock(DatabusAuthorizer.class), "systemOwnerId", acceptAll, MoreExecutors.newDirectExecutorService(),
                3, masterPartitioner, new MetricRegistry(), Clock.systemUTC());

        List<UpdateRef> updateRefs = Lists.newArrayListWithCapacity(4);
        for (int i=0; i < 4; i++) {
            updateRefs.add(new UpdateRef("test-table", "key" + i, TimeUUIDs.newUUID(), ImmutableSet.of()));
        }

        testDatabus.writeEvents(updateRefs);

        assertEquals(eventsStored, ImmutableSetMultimap.builder()
                .putAll(ChannelNames.getMasterFanoutChannel(0), updateRefs.get(0), updateRefs.get(3))
                .put(ChannelNames.getMasterFanoutChannel(1), updateRefs.get(1))
                .put(ChannelNames.getMasterFanoutChannel(2), updateRefs.get(2))
                .build());
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

    private static void addToPoll(String eventId, String table, String key, boolean redundant, EventSink sink,
                                  TestDataProvider testDataProvider) {
        assertTrue(sink.remaining() > 0);
        EventSink.Status status = sink.accept(newEvent(eventId, table, key, TimeUUIDs.newUUID()));
        assertEquals(status, EventSink.Status.ACCEPTED_CONTINUE);

        Map<String, Object> content = entity(table, key, ImmutableMap.of("rating", "5"));
        DataProvider.AnnotatedContent annotatedContent = mock(DataProvider.AnnotatedContent.class);
        when(annotatedContent.getContent()).thenReturn(content);
        when(annotatedContent.isChangeDeltaRedundant(any(UUID.class))).thenReturn(redundant);
        testDataProvider.add(annotatedContent);
    }

    private static <T> Matcher<Collection<T>> containsExactly(final Collection<T> expected) {
        return new BaseMatcher<Collection<T>>() {
            @Override
            public boolean matches(Object o) {
                return o != null &&
                        o instanceof Collection &&
                        ImmutableSet.copyOf((Collection) o).equals(ImmutableSet.copyOf(expected));
            }

            @Override
            public void describeTo(Description description) {
                description.appendText("contains exactly ").appendValue(expected);

            }
        };
    }
}
