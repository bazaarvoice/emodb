package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.api.DefaultSubscription;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.bazaarvoice.emodb.table.db.test.InMemoryTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class SubscriptionEvaluatorTest {
    /**
     * This tests if the tag based subscriptions are correctly matched with their events.
     */
    @Test
    public void testSubscriptionEvaluator() {
        DataProvider dataProvider = mock(DataProvider.class);
        when(dataProvider.getTable(anyString())).thenReturn(new InMemoryTable("table1",
                new TableOptionsBuilder().setPlacement("app_global:ugc").build(), Maps.<String, Object>newHashMap()));
        SubscriptionEvaluator subscriptionEvaluator = new SubscriptionEvaluator(dataProvider,
                mock(RateLimitedLogFactory.class));
        UpdateRef updateRef = new UpdateRef("table1", "some-key", TimeUUIDs.newUUID(), ImmutableSet.of("ignore", "ETL"));

        // Subscription that skips "ignore" events
        // Condition is only based on tags - the events should not contain any "ignore" tags
        Condition skipIgnoreEvents = Conditions.not(Conditions.mapBuilder().matches(UpdateRef.TAGS_NAME, Conditions.containsAny("ignore")).build());
        Subscription skipIgnoreSubscription = new DefaultSubscription("test-tags", skipIgnoreEvents,
                DateTime.now().withDurationAdded(Duration.standardDays(1), 1).toDate(), Duration.standardHours(1));
        assertFalse(subscriptionEvaluator.matches(skipIgnoreSubscription, UpdateRefSerializer.toByteBuffer(updateRef)));
        // The following databus event should match, as it doesn't have "ignore" tag
        UpdateRef updateRef2 = new UpdateRef("table1", "some-key", TimeUUIDs.newUUID(), ImmutableSet.of("ETL"));
        assertTrue(subscriptionEvaluator.matches(skipIgnoreSubscription, UpdateRefSerializer.toByteBuffer(updateRef2)));
        UpdateRef updateRef3 = new UpdateRef("table1", "some-key", TimeUUIDs.newUUID(), ImmutableSet.<String>of());
        assertTrue(subscriptionEvaluator.matches(skipIgnoreSubscription, UpdateRefSerializer.toByteBuffer(updateRef3)));

        // Subscription that explicitly asks for "ignore" events
        Condition getIgnoreEvents = Conditions.mapBuilder().matches(UpdateRef.TAGS_NAME, Conditions.containsAny("ignore")).build();
        Subscription getIgnoreSubscription = new DefaultSubscription("test-tags", getIgnoreEvents,
                DateTime.now().withDurationAdded(Duration.standardDays(1), 1).toDate(), Duration.standardHours(1));
        assertTrue(subscriptionEvaluator.matches(getIgnoreSubscription, UpdateRefSerializer.toByteBuffer(updateRef)));
        // The following databus event should *not* match, as it doesn't have "ignore" tag
        updateRef2 = new UpdateRef("table1", "some-key", TimeUUIDs.newUUID(), ImmutableSet.of("ETL"));
        assertFalse(subscriptionEvaluator.matches(getIgnoreSubscription, UpdateRefSerializer.toByteBuffer(updateRef2)));
        updateRef3 = new UpdateRef("table1", "some-key", TimeUUIDs.newUUID(), ImmutableSet.<String>of());
        assertFalse(subscriptionEvaluator.matches(getIgnoreSubscription, UpdateRefSerializer.toByteBuffer(updateRef3)));
    }
}
