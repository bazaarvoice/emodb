package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLog;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLogFactory;
import com.bazaarvoice.emodb.common.dropwizard.time.ClockTicker;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.event.api.EventData;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Copies events from the "__system_bus:master" event channel or an inbound replication channel to the individual
 * subscription event channels.
 * <p>
 * Each source channel is handled by a single process in the EmoDB cluster.  Generally this copy process is fast enough
 * (and I/O bound) that it's not necessary to spread the work across different servers, but if that changes we can
 * spread writes across multiple source channels (eg. __system_bus:master1, __system_bus:master2, etc.).
 */
public class DefaultFanout extends AbstractScheduledService {
    private static final Logger _log = LoggerFactory.getLogger(DefaultFanout.class);

    private static final int FLUSH_EVENTS_THRESHOLD = 500;

    private final String _name;
    private final EventSource _eventSource;
    private final Function<Multimap<String, ByteBuffer>, Void> _eventSink;
    private final boolean _replicateOutbound;
    private final PartitionSelector _outboundPartitionSelector;
    private final Duration _sleepWhenIdle;
    private final Supplier<Iterable<OwnedSubscription>> _subscriptionsSupplier;
    private final DataCenter _currentDataCenter;
    private final RateLimitedLog _rateLimitedLog;
    private final SubscriptionEvaluator _subscriptionEvaluator;
    private final Meter _eventsRead;
    private final Meter _eventsWrittenLocal;
    private final Meter _eventsWrittenOutboundReplication;
    private final Meter _subscriptionMatchEvaluations;
    private final Timer _totalCopyTimer;
    private final Timer _fetchEventsTimer;
    private final Timer _fetchSubscriptionsTimer;
    private final Timer _fanoutTimer;
    private final Timer _e2eFanoutTimer;
    private final Timer _matchSubscriptionsTimer;
    private final Timer _replicateTimer;
    private final Timer _fetchMatchEventDataTimer;
    private final Timer _eventFlushTimer;
    private final Clock _clock;
    private final Stopwatch _lastLagStopwatch;
    private final FanoutLagMonitor.Lag _lagGauge;
    private int _lastLagSeconds = -1;

    private final ExecutorService _fanoutPool;

    public DefaultFanout(String name,
                         String partitionName,
                         EventSource eventSource,
                         Function<Multimap<String, ByteBuffer>, Void> eventSink,
                         @Nullable PartitionSelector outboundPartitionSelector,
                         Duration sleepWhenIdle,
                         Supplier<Iterable<OwnedSubscription>> subscriptionsSupplier,
                         DataCenter currentDataCenter,
                         RateLimitedLogFactory logFactory,
                         SubscriptionEvaluator subscriptionEvaluator,
                         FanoutLagMonitor fanoutLagMonitor,
                         MetricRegistry metricRegistry, Clock clock) {
        _name = checkNotNull(name, "name");
        checkNotNull(partitionName, "partitionName");
        _eventSource = checkNotNull(eventSource, "eventSource");
        _eventSink = checkNotNull(eventSink, "eventSink");
        _replicateOutbound = outboundPartitionSelector != null;
        _outboundPartitionSelector = outboundPartitionSelector;
        _sleepWhenIdle = checkNotNull(sleepWhenIdle, "sleepWhenIdle");
        _subscriptionsSupplier = checkNotNull(subscriptionsSupplier, "subscriptionsSupplier");
        _currentDataCenter = checkNotNull(currentDataCenter, "currentDataCenter");
        _subscriptionEvaluator = checkNotNull(subscriptionEvaluator, "subscriptionEvaluator");

        _rateLimitedLog = logFactory.from(_log);
        _eventsRead = newEventMeter("read", metricRegistry);
        _eventsWrittenLocal = newEventMeter("written-local", metricRegistry);
        _eventsWrittenOutboundReplication = newEventMeter("written-outbound-replication", metricRegistry);
        _subscriptionMatchEvaluations = newEventMeter("subscription-match-evaluations", metricRegistry);
        _totalCopyTimer = metricRegistry.timer(metricName("total-copy"));
        _fetchEventsTimer = metricRegistry.timer(metricName("fetch-events"));
        _fetchSubscriptionsTimer = metricRegistry.timer(metricName("fetch-subscriptions"));
        _fanoutTimer = metricRegistry.timer(metricName("fanout"));
        _e2eFanoutTimer = metricRegistry.timer(metricName("e2e-fanout"));
        _matchSubscriptionsTimer = metricRegistry.timer(metricName("match-subscriptions"));
        _replicateTimer = metricRegistry.timer(metricName("replicate"));
        _fetchMatchEventDataTimer = metricRegistry.timer(metricName("fetch-match-event-data"));
        _eventFlushTimer = metricRegistry.timer(metricName("flush-events"));

        _lagGauge = checkNotNull(fanoutLagMonitor, "fanoutLagMonitor").createForFanout(name, partitionName);
        _lastLagStopwatch = Stopwatch.createStarted(ClockTicker.getTicker(clock));
        _clock = clock;
        ServiceFailureListener.listenTo(this, metricRegistry);

        _fanoutPool = Executors.newFixedThreadPool(
            8,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("fanout-%d").build()
        );
    }

    private Meter newEventMeter(String name, MetricRegistry metricRegistry) {
        return metricRegistry.meter(metricName(name));
    }

    private String metricName(String name) {
        return MetricRegistry.name("bv.emodb.databus", "DefaultFanout", name, _name);
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, _sleepWhenIdle.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void runOneIteration() {
        try {
            //noinspection StatementWithEmptyBody
            while (isRunning() && copyEvents()) {
                // Loop w/o sleeping as long as we keep finding events.
            }
        } catch (Throwable t) {
            // Fanout runs in a continuous loop.  If we get into a bad state, use the rate limited log to avoid
            // flooding the logs with a continuous stream of error messages.  Include the event source name in the
            // message template so we rate limit each event source independently.
            _rateLimitedLog.error(t, "Unexpected fanout exception copying from " + _name + ": {}", t);
            stopAsync();  // Give up leadership temporarily.  Maybe another server will have more success.
        }
    }

    @Override
    protected void shutDown() throws Exception {
        // Leadership lost, stop posting fanout lag
        _lagGauge.close();
    }

    private boolean copyEvents() {
        try (Timer.Context ignored = _totalCopyTimer.time()) {
            // Use peek() not poll() since LeaderSelector ensures we're not competing with other processes for claims.
            final Timer.Context peekTime = _fetchEventsTimer.time();
            List<EventData> rawEvents = _eventSource.get(1000);
            peekTime.stop();

            // If no events, sleep a little while before doing any more work to allow new events to arrive.
            if (rawEvents.isEmpty()) {
                // Update the lag metrics to indicate there is no lag
                updateLagMetrics(null);
                return false;
            }

            // Last chance to check that we are the leader before doing anything that would be bad if we aren't.
            return isRunning() && copyEvents(rawEvents);
        }
    }


    @VisibleForTesting
    boolean copyEvents(List<EventData> rawEvents) {
        // Read the list of subscriptions *after* reading events from the event store to avoid race conditions with
        // creating a new subscription.
        final Timer.Context subTime = _fetchSubscriptionsTimer.time();
        Iterable<OwnedSubscription> subscriptions = _subscriptionsSupplier.get();
        subTime.stop();

        List<Date> lastMatchEventBatchTimes = Collections.synchronizedList(Lists.newArrayList());
        
        try(final Timer.Context ignored = _e2eFanoutTimer.time()) {
            final List<Future<?>> futures = new LinkedList<>();
            // Copy the events to all the destination channels.
            for (final List<EventData> rawEventPartition : Lists.partition(rawEvents, (int) Math.ceil(1.0 * rawEvents.size() / 8))) {
                futures.add(_fanoutPool.submit(() -> {
                    try {
                        // multimap is not threadsafe
                        final List<String> eventKeys = new LinkedList<>();
                        final ListMultimap<String, ByteBuffer> eventsByChannel = LinkedListMultimap.create();
                        SubscriptionEvaluator.MatchEventData lastMatchEventData = null;
                        int numOutboundReplicationEvents = 0;
                        try (Timer.Context ignored1 = _fanoutTimer.time()) {
                            for (EventData rawEvent : rawEventPartition) {
                                ByteBuffer eventData = rawEvent.getData();

                                SubscriptionEvaluator.MatchEventData matchEventData;
                                try (Timer.Context ignored2 = _fetchMatchEventDataTimer.time()) {
                                    matchEventData = _subscriptionEvaluator.getMatchEventData(eventData);
                                } catch (OrphanedEventException e) {
                                    // There's a 2 second window where a race condition exists such that a newly created
                                    // table may exist but due to caching the table may be cached as unknown.  To allow
                                    // plenty of room for error wait until over 30 seconds after the event was written
                                    // before dropping the event.  After this the event must be orphaned because
                                    // the associated table was dropped.
                                    if (e.getEventTime().until(_clock.instant(), ChronoUnit.SECONDS) > 30) {
                                        eventKeys.add(rawEvent.getId());
                                    }
                                    continue;
                                }

                                eventKeys.add(rawEvent.getId());

                                // Copy to subscriptions in the current data center.
                                Timer.Context matchTime = _matchSubscriptionsTimer.time();
                                int subscriptionCount = 0;
                                for (OwnedSubscription subscription : subscriptions) {
                                    subscriptionCount += 1;
                                    if (_subscriptionEvaluator.matches(subscription, matchEventData)) {
                                        eventsByChannel.put(subscription.getName(), eventData);
                                    }
                                }
                                matchTime.stop();
                                _subscriptionMatchEvaluations.mark(subscriptionCount);

                                // Copy to queues for eventual delivery to remote data centers.
                                try (Timer.Context ignored4 = _replicateTimer.time()) {
                                    if (_replicateOutbound) {
                                        for (DataCenter dataCenter : matchEventData.getTable().getDataCenters()) {
                                            if (!dataCenter.equals(_currentDataCenter)) {
                                                int partition = _outboundPartitionSelector.getPartition(matchEventData.getKey());
                                                String channel = ChannelNames.getReplicationFanoutChannel(dataCenter, partition);
                                                eventsByChannel.put(channel, eventData);
                                                numOutboundReplicationEvents++;
                                            }
                                        }
                                    }
                                }

                                // Flush to cap the amount of memory used to buffer events.
                                if (eventsByChannel.size() >= FLUSH_EVENTS_THRESHOLD) {
                                    flush(eventKeys, eventsByChannel, numOutboundReplicationEvents);
                                    numOutboundReplicationEvents = 0;
                                }

                                // Track the final match event data record returned
                                lastMatchEventData = matchEventData;
                            }

                            // Final flush.
                            flush(eventKeys, eventsByChannel, numOutboundReplicationEvents);

                            if (lastMatchEventData != null) {
                                lastMatchEventBatchTimes.add(lastMatchEventData.getEventTime());
                            }
                        }
                    } catch (Throwable t) {
                        _log.error("Uncaught exception in fanout pool. Thread will die, but it should be replaced.", t);
                        throw t;
                    }
                }));
            }

            for (final Future<?> future : futures) {
                Futures.getUnchecked(future);
            }

            // Update the lag metrics based on the last event returned.  This isn't perfect for several reasons:
            // 1. In-order delivery is not guaranteed
            // 2. The event time is based on the change ID which is close-to but not precisely the time the update occurred
            // 3. Injected events have artificial change IDs which don't correspond to any clock-based time
            // However, this is still a useful metric because:
            // 1. Delivery is in-order the majority of the time
            // 2. Change IDs are typically within milliseconds of update times
            // 3. Injected events are extremely rare and should be avoided outside of testing anyway
            // 4. The lag only becomes a concern on the scale of minutes, far above the uncertainty introduced by the above
            Date lastMatchEventTime = lastMatchEventBatchTimes.stream().max(Date::compareTo).orElse(null);
            if (lastMatchEventTime != null) {
                updateLagMetrics(lastMatchEventTime);
            }
        }

        return true;
    }

    private void updateLagMetrics(@Nullable Date eventTime) {
        int lagSeconds = eventTime == null ? 0 : (int) TimeUnit.MILLISECONDS.toSeconds(_clock.millis() - eventTime.getTime());
        // As a performance savings only update the metric if both of the following are true:
        // 1. It has been more than 5 seconds since the last time the metric was updated
        // 2. The lag changed since the last posting

        if (lagSeconds != _lastLagSeconds && _lastLagStopwatch.elapsed(TimeUnit.SECONDS) >= 5) {
            _lagGauge.setLagSeconds(lagSeconds);
            _lastLagSeconds = lagSeconds;
            _lastLagStopwatch.reset().start();
        }
    }

    private void flush(List<String> eventKeys, Multimap<String, ByteBuffer> eventsByChannel,
                       int numOutboundReplicationEvents) {
        try (Timer.Context ignore = _eventFlushTimer.time()) {
            if (!eventsByChannel.isEmpty()) {
                _eventSink.apply(eventsByChannel);
                _eventsWrittenLocal.mark(eventsByChannel.size() - numOutboundReplicationEvents);
                _eventsWrittenOutboundReplication.mark(numOutboundReplicationEvents);
                eventsByChannel.clear();
            }
            if (!eventKeys.isEmpty()) {
                _eventSource.delete(eventKeys);
                _eventsRead.mark(eventKeys.size());
                eventKeys.clear();
            }
        }
    }
}
