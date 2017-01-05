package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.auth.DatabusAuthorizer;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
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
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.MoreExecutors;
import org.joda.time.Duration;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
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
                mock(LifeCycleRegistry.class), mock(EventBus.class), mock(DataProvider.class), mockSubscriptionDao,
                mock(DatabusEventStore.class), mock(SubscriptionEvaluator.class), mock(JobService.class),
                mock(JobHandlerRegistry.class), mock(DatabusAuthorizer.class), "replication", ignoreReEtl, mock(ExecutorService.class),
                mock(MetricRegistry.class), Clock.systemUTC());
        Condition originalCondition = Conditions.mapBuilder().contains("foo", "bar").build();
        testDatabus.subscribe("id", "test-subscription", originalCondition, Duration.standardDays(7),
                Duration.standardDays(7));
        // Skip databus events tagged with "re-etl"
        Condition skipIgnoreTags = Conditions.not(Conditions.mapBuilder().matches(UpdateRef.TAGS_NAME, Conditions.containsAny("re-etl")).build());
        Condition expectedConditionToSkipIgnore = Conditions.and(originalCondition, skipIgnoreTags);
        verify(mockSubscriptionDao).insertSubscription("id", "test-subscription", expectedConditionToSkipIgnore,
                Duration.standardDays(7), Duration.standardDays(7));
        verify(mockSubscriptionDao).getSubscription("test-subscription");
        verifyNoMoreInteractions(mockSubscriptionDao);

        // reset mocked subscription DAO so it doesn't carry information about old interactions
        reset(mockSubscriptionDao);
        // Test condition is unchanged if includeDefaultJoinFilter is set to false
        testDatabus.subscribe("id", "test-subscription", originalCondition, Duration.standardDays(7),
                Duration.standardDays(7), false);
        verify(mockSubscriptionDao).insertSubscription("id", "test-subscription", originalCondition, Duration.standardDays(7),
                Duration.standardDays(7));
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
                mock(LifeCycleRegistry.class), mock(EventBus.class), new TestDataProvider().add(annotatedContent), mock(SubscriptionDAO.class),
                eventStore, mock(SubscriptionEvaluator.class), mock(JobService.class),
                mock(JobHandlerRegistry.class), mock(DatabusAuthorizer.class), "systemOwnerId", ignoreReEtl, MoreExecutors.sameThreadExecutor(),
                mock(MetricRegistry.class), Clock.systemUTC());

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
                mock(LifeCycleRegistry.class), mock(EventBus.class), new TestDataProvider().add(annotatedContent), mock(SubscriptionDAO.class),
                eventStore, mock(SubscriptionEvaluator.class), mock(JobService.class),
                mock(JobHandlerRegistry.class), mock(DatabusAuthorizer.class), "systemOwnerId", ignoreReEtl, MoreExecutors.sameThreadExecutor(),
                mock(MetricRegistry.class), Clock.systemUTC());

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
                mock(LifeCycleRegistry.class), mock(EventBus.class), new TestDataProvider().add(annotatedContent), mock(SubscriptionDAO.class),
                eventStore, mock(SubscriptionEvaluator.class), mock(JobService.class),
                mock(JobHandlerRegistry.class), mock(DatabusAuthorizer.class), "systemOwnerId", ignoreReEtl, MoreExecutors.sameThreadExecutor(),
                mock(MetricRegistry.class), Clock.systemUTC());

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
