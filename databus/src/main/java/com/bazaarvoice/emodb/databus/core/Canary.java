package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.common.dropwizard.metrics.MetricsGroup;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.table.db.ClusterInfo;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Polls and acks events from all emo tables once every second (approx.)
 * <p/>
 * Databus canary is a single process in the EmoDB cluster.
 * Since there's only one subscription - "__system_bus:canary" and all subscriptions are routed to a single Databus server,
 * it's reasonable to use leader election between the EmoDB servers so that only one server polls the canary Databus subscription.
 */
public class Canary extends AbstractScheduledService {
    private static final Logger _log = LoggerFactory.getLogger(Canary.class);

    private static final Duration POLL_INTERVAL = Duration.standardSeconds(1);
    private static final Duration CLAIM_TTL = Duration.standardSeconds(30);
    private static final int EVENTS_LIMIT = 50;

    private final RateLimitedLog _rateLimitedLog;
    private final Databus _databus;
    private final MetricsGroup _timers;
    private final String _timerName;
    private final String _subscriptionName;
    private final Condition _subscriptionCondition;

    public Canary(ClusterInfo cluster, Condition subscriberCondition, Databus databus, RateLimitedLogFactory logFactory, MetricRegistry metricRegistry) {
        _databus = checkNotNull(databus, "databus");
        _timers = new MetricsGroup(metricRegistry);
        checkNotNull(cluster, "cluster");
        _subscriptionName = ChannelNames.getMasterCanarySubscription(cluster.getCluster());
        _subscriptionCondition = checkNotNull(subscriberCondition, "subscriptionCondition");
        _timerName = newTimerName("readEventsByCanaryPoll-" + cluster.getClusterMetric());
        _rateLimitedLog = logFactory.from(_log);
        createCanarySubscription();
        ServiceFailureListener.listenTo(this, metricRegistry);
    }

    private String newTimerName(String name) {
        return MetricRegistry.name("bv.emodb.databus", "DatabusCanary", name, "readEvents");
    }

    private void createCanarySubscription() {
        // Except for resetting the ttl, recreating a subscription that already exists has no effect.
        // Assume that multiple servers that manage the same subscriptions can each attempt to create
        // the subscription at startup.  The subscription should last basically forever.
        // Note: make sure that we don't ignore any events
        _databus.subscribe(_subscriptionName, _subscriptionCondition,
                Duration.standardDays(3650), DatabusChannelConfiguration.CANARY_TTL, false);
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, POLL_INTERVAL.getMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void shutDown() throws Exception {
        _timers.close();  // Lost leadership.  Stop reporting metrics so we don't conflict with the new leader.
    }

    @Override
    protected void runOneIteration() {
        try {
            //noinspection StatementWithEmptyBody
            while (isRunning() && pollAndAckEvents()) {
                // Loop w/o sleeping as long as we keep finding events
            }
        } catch (Throwable t) {
            _rateLimitedLog.error(t, "Unexpected canary exception: {}", t);
            stop();  // Give up leadership temporarily.  Maybe another server will have more success.
        }
    }

    private boolean pollAndAckEvents() {
        // Poll for events on the canary bus subscription
        long startTime = System.nanoTime();
        List<Event> events = _databus.poll(_subscriptionName, CLAIM_TTL, EVENTS_LIMIT);
        long endTime = System.nanoTime();
        trackAverageEventDuration(endTime - startTime, events.size());

        // If no events, sleep a second before returning and continuing to poll
        if (events.isEmpty()) {
            return false;
        }

        // Last chance to check that we are the leader before doing anything that would be bad if we aren't.
        if (!isRunning()) {
            return false;
        }

        // Ack these events
        _databus.acknowledge(_subscriptionName, Lists.transform(events, new Function<Event, String>() {
            @Override
            public String apply(Event event) {
                return event.getEventKey();
            }
        }));

        return true;
    }

    private void trackAverageEventDuration(long durationInNs, int numEvents) {
        if (numEvents == 0) {
            return;
        }

        long durationPerEvent = (durationInNs + numEvents - 1) / numEvents;  // round up

        _timers.beginUpdates();
        Timer timer = _timers.timer(_timerName, TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS);
        for (int i = 0; i < numEvents; i++) {
            timer.update(durationPerEvent, TimeUnit.NANOSECONDS);
        }
        _timers.endUpdates();
    }
}
