package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.table.db.ClusterInfo;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@SuppressWarnings("unchecked")
public class CanaryTest {

    private Databus _databus;
    private Canary _canary;
    private Runnable _iterationRunnable;

    @BeforeMethod
    public void setUp() throws Exception {
        _databus = mock(Databus.class);

        ClusterInfo clusterInfo = new ClusterInfo("cluster", "metric");
        Condition condition = Conditions.alwaysTrue();
        RateLimitedLogFactory rateLimitedLogFactory = mock(RateLimitedLogFactory.class);
        when(rateLimitedLogFactory.from(any(Logger.class))).thenReturn(mock(RateLimitedLog.class));

        ScheduledExecutorService service = mock(ScheduledExecutorService.class);
        when(service.scheduleWithFixedDelay(any(Runnable.class), eq(0L), eq(1000L), eq(TimeUnit.MILLISECONDS)))
                .thenAnswer(invocationOnMock -> {
                    _iterationRunnable = (Runnable) invocationOnMock.getArguments()[0];
                    return mock(ScheduledFuture.class);
                });

        _canary = new Canary(clusterInfo, condition, _databus, rateLimitedLogFactory, new MetricRegistry(), service);

        verify(_databus).subscribe("__system_bus:canary-cluster", Conditions.alwaysTrue(),
                Duration.ofDays(3650), DatabusChannelConfiguration.CANARY_TTL, false);

        _canary.startAsync();

        // Starting the canary sends a single initialization runnable to the service, so execute it now.
        ArgumentCaptor<Runnable> runnable = ArgumentCaptor.forClass(Runnable.class);
        verify(service).execute(runnable.capture());
        runnable.getValue().run();

        _canary.awaitRunning(1, TimeUnit.SECONDS);
        assertTrue(_canary.isRunning());

        // Verify the first callable was scheduled on startup
        assertNotNull(_iterationRunnable);
    }

    @BeforeMethod
    public void tearDown() {
        // Verify the canary is still running
        assertTrue(_canary.isRunning());
        // No need to shut down the canary since it is running with a mocked executor service
        verifyNoMoreInteractions(_databus);
    }


    @Test
    public void testIterationWithNoEvents() throws Exception {
        when(_databus.poll("__system_bus:canary-cluster", Duration.ofSeconds(30), 50))
                .thenReturn(new PollResult(Iterators.emptyIterator(), 0, false));

        _iterationRunnable.run();

        verify(_databus).poll("__system_bus:canary-cluster", Duration.ofSeconds(30), 50);
    }

    @Test
    public void testIterationWithEventsInOnePoll() throws Exception {
        List<Event> events = Lists.newArrayListWithCapacity(20);
        List<String> eventIds = Lists.newArrayListWithCapacity(20);
        for (int i=0; i < 20; i++) {
            String eventId = String.valueOf(i);
            events.add(new Event(eventId, ImmutableMap.of(), ImmutableList.of()));
            eventIds.add(eventId);
        }
        when(_databus.poll("__system_bus:canary-cluster", Duration.ofSeconds(30), 50))
                .thenReturn(new PollResult(events.iterator(), events.size(), false));

        _iterationRunnable.run();

        verify(_databus).poll("__system_bus:canary-cluster", Duration.ofSeconds(30), 50);
        verify(_databus).acknowledge("__system_bus:canary-cluster", eventIds);
    }

    @Test
    public void testIterationWithEventsInMultiplePolls() throws Exception {
        List<List<Event>> events = Lists.newArrayListWithCapacity(3);
        List<List<String>> eventIds = Lists.newArrayListWithCapacity(3);

        for (int batch=0; batch < 3; batch++) {
            List<Event> batchEvents = Lists.newArrayListWithCapacity(50);
            List<String> batchEventIds = Lists.newArrayListWithCapacity(50);
            events.add(batchEvents);
            eventIds.add(batchEventIds);

            for (int i=0; i < 50; i++) {
                String eventId = String.valueOf(batch * 50 + i);
                batchEvents.add(new Event(eventId, ImmutableMap.of(), ImmutableList.of()));
                batchEventIds.add(eventId);
            }
        }

        when(_databus.poll("__system_bus:canary-cluster", Duration.ofSeconds(30), 50))
                .thenReturn(new PollResult(events.get(0).iterator(), events.get(0).size(), true))
                .thenReturn(new PollResult(events.get(1).iterator(), events.get(1).size(), true))
                .thenReturn(new PollResult(events.get(2).iterator(), events.get(2).size(), false));

        _iterationRunnable.run();

        verify(_databus, times(3)).poll("__system_bus:canary-cluster", Duration.ofSeconds(30), 50);
        for (List<String> batchEventIds : eventIds) {
            verify(_databus).acknowledge("__system_bus:canary-cluster", batchEventIds);
        }
    }

    @Test
    public void testIterationWithDiscardedEvents() throws Exception {
        when(_databus.poll("__system_bus:canary-cluster", Duration.ofSeconds(30), 50))
                .thenReturn(new PollResult(Iterators.emptyIterator(), 0, true))
                .thenReturn(new PollResult(Iterators.emptyIterator(), 0, true))
                .thenReturn(new PollResult(Iterators.emptyIterator(), 0, false));

        _iterationRunnable.run();

        verify(_databus, times(3)).poll("__system_bus:canary-cluster", Duration.ofSeconds(30), 50);
    }
}
