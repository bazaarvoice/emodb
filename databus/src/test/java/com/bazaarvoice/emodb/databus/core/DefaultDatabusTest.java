package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.EventBus;
import org.joda.time.Duration;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class DefaultDatabusTest {
    @Test
    public void testSubscriptionCreation() {
        // Currently, by default, ignoreSuppressedEvents is set to true,
        // which means that we append the skipIgnored tags condition to the original table filter
        // unless the caller explicitly sets the ignoreSuppressedEvents to false.
        // This is just an interim setup to be backwards compatible. Soon we will deprecate the
        // ignoreSuppressedEvents flag.

        SubscriptionDAO mockSubscriptionDao = mock(SubscriptionDAO.class);
        DefaultDatabus testDatabus = new DefaultDatabus(
                mock(LifeCycleRegistry.class), mock(EventBus.class), mock(DataProvider.class), mockSubscriptionDao,
                mock(DatabusEventStore.class), mock(SubscriptionEvaluator.class), mock(JobService.class),
                mock(JobHandlerRegistry.class), mock(MetricRegistry.class));
        Condition originalCondition = Conditions.alwaysTrue();
        testDatabus.subscribe("test-subscription", originalCondition, Duration.standardDays(7),
                Duration.standardDays(7));
        // Skip databus events tagged with "re-etl"
        Condition skipIgnoreTags = Conditions.not(Conditions.mapBuilder().matches(UpdateRef.TAGS_NAME, Conditions.containsAny("re-etl")).build());
        Condition expectedConditionToSkipIgnore = Conditions.and(originalCondition, skipIgnoreTags);
        verify(mockSubscriptionDao).insertSubscription("test-subscription", expectedConditionToSkipIgnore,
                Duration.standardDays(7), Duration.standardDays(7));
        verifyNoMoreInteractions(mockSubscriptionDao);

        // reset mocked subscription DAO so it doesn't carry information about old interactions
        reset(mockSubscriptionDao);
        // Test condition is unchanged if ignoreSuppressedEvents is set to false
        testDatabus.subscribe("test-subscription", originalCondition, Duration.standardDays(7),
                Duration.standardDays(7), false);
        verify(mockSubscriptionDao).insertSubscription("test-subscription", originalCondition, Duration.standardDays(7),
                Duration.standardDays(7));
        verifyNoMoreInteractions(mockSubscriptionDao);
    }
}
