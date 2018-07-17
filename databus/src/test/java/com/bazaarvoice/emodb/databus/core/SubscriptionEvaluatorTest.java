package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.auth.ConstantDatabusAuthorizer;
import com.bazaarvoice.emodb.databus.model.DefaultOwnedSubscription;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.bazaarvoice.emodb.table.db.test.InMemoryTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

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
                ConstantDatabusAuthorizer.ALLOW_ALL, mock(RateLimitedLogFactory.class));
        UpdateRef updateRef = new UpdateRef("table1", "some-key", TimeUUIDs.newUUID(), ImmutableSet.of("ignore", "ETL"));

        // Subscription that skips "ignore" events
        // Condition is only based on tags - the events should not contain any "ignore" tags
        Condition skipIgnoreEvents = Conditions.not(Conditions.mapBuilder().matches(UpdateRef.TAGS_NAME, Conditions.containsAny("ignore")).build());
        OwnedSubscription skipIgnoreSubscription = new DefaultOwnedSubscription("test-tags", skipIgnoreEvents,
                Date.from(Instant.now().plus(Duration.ofDays(1))), Duration.ofHours(1), "id");
        assertFalse(subscriptionEvaluator.matches(skipIgnoreSubscription, UpdateRefSerializer.toByteBuffer(updateRef)));
        // The following databus event should match, as it doesn't have "ignore" tag
        UpdateRef updateRef2 = new UpdateRef("table1", "some-key", TimeUUIDs.newUUID(), ImmutableSet.of("ETL"));
        assertTrue(subscriptionEvaluator.matches(skipIgnoreSubscription, UpdateRefSerializer.toByteBuffer(updateRef2)));
        UpdateRef updateRef3 = new UpdateRef("table1", "some-key", TimeUUIDs.newUUID(), ImmutableSet.<String>of());
        assertTrue(subscriptionEvaluator.matches(skipIgnoreSubscription, UpdateRefSerializer.toByteBuffer(updateRef3)));

        // Subscription that explicitly asks for "ignore" events
        Condition getIgnoreEvents = Conditions.mapBuilder().matches(UpdateRef.TAGS_NAME, Conditions.containsAny("ignore")).build();
        OwnedSubscription getIgnoreSubscription = new DefaultOwnedSubscription("test-tags", getIgnoreEvents,
                Date.from(Instant.now().plus(Duration.ofDays(1))), Duration.ofHours(1), "id");
        assertTrue(subscriptionEvaluator.matches(getIgnoreSubscription, UpdateRefSerializer.toByteBuffer(updateRef)));
        // The following databus event should *not* match, as it doesn't have "ignore" tag
        updateRef2 = new UpdateRef("table1", "some-key", TimeUUIDs.newUUID(), ImmutableSet.of("ETL"));
        assertFalse(subscriptionEvaluator.matches(getIgnoreSubscription, UpdateRefSerializer.toByteBuffer(updateRef2)));
        updateRef3 = new UpdateRef("table1", "some-key", TimeUUIDs.newUUID(), ImmutableSet.<String>of());
        assertFalse(subscriptionEvaluator.matches(getIgnoreSubscription, UpdateRefSerializer.toByteBuffer(updateRef3)));
    }

    @Test
    public void testUnauthorized() {
        DataProvider dataProvider = mock(DataProvider.class);
        when(dataProvider.getTable(anyString())).thenReturn(new InMemoryTable("table1",
                new TableOptionsBuilder().setPlacement("app_global:ugc").build(), Maps.<String, Object>newHashMap()));
        SubscriptionEvaluator subscriptionEvaluator = new SubscriptionEvaluator(dataProvider,
                ConstantDatabusAuthorizer.DENY_ALL, mock(RateLimitedLogFactory.class));
        UpdateRef updateRef = new UpdateRef("table1", "some-key", TimeUUIDs.newUUID(), ImmutableSet.of("ignore", "ETL"));

        // No condition, even alwaysTrue(), matches when the authorizer doesn't have permission
        OwnedSubscription allSubscription = new DefaultOwnedSubscription("all", Conditions.alwaysTrue(),
                Date.from(Instant.now().plus(Duration.ofDays(1))), Duration.ofHours(1), "id");
        assertFalse(subscriptionEvaluator.matches(allSubscription, UpdateRefSerializer.toByteBuffer(updateRef)));
    }
}
