package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
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
    private final Duration _sleepWhenIdle;
    private final Supplier<Iterable<OwnedSubscription>> _subscriptionsSupplier;
    private final DataCenter _currentDataCenter;
    private final RateLimitedLog _rateLimitedLog;
    private final SubscriptionEvaluator _subscriptionEvaluator;
    private final Meter _eventsRead;
    private final Meter _eventsWrittenLocal;
    private final Meter _eventsWrittenOutboundReplication;

    public DefaultFanout(String name,
                         EventSource eventSource,
                         Function<Multimap<String, ByteBuffer>, Void> eventSink,
                         boolean replicateOutbound,
                         Duration sleepWhenIdle,
                         Supplier<Iterable<OwnedSubscription>> subscriptionsSupplier,
                         DataCenter currentDataCenter,
                         RateLimitedLogFactory logFactory,
                         SubscriptionEvaluator subscriptionEvaluator,
                         MetricRegistry metricRegistry) {
        _name = checkNotNull(name, "name");
        _eventSource = checkNotNull(eventSource, "eventSource");
        _eventSink = checkNotNull(eventSink, "eventSink");
        _replicateOutbound = replicateOutbound;
        _sleepWhenIdle = checkNotNull(sleepWhenIdle, "sleepWhenIdle");
        _subscriptionsSupplier = checkNotNull(subscriptionsSupplier, "subscriptionsSupplier");
        _currentDataCenter = checkNotNull(currentDataCenter, "currentDataCenter");
        _subscriptionEvaluator = checkNotNull(subscriptionEvaluator, "subscriptionEvaluator");

        _rateLimitedLog = logFactory.from(_log);
        _eventsRead = newEventMeter("read", metricRegistry);
        _eventsWrittenLocal = newEventMeter("written-local", metricRegistry);
        _eventsWrittenOutboundReplication = newEventMeter("written-outbound-replication", metricRegistry);
        ServiceFailureListener.listenTo(this, metricRegistry);
    }

    private Meter newEventMeter(String name, MetricRegistry metricRegistry) {
        String metricName = MetricRegistry.name("bv.emodb.databus", "DefaultFanout", name, _name);
        return metricRegistry.meter(metricName);
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, _sleepWhenIdle.getMillis(), TimeUnit.MILLISECONDS);
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
            stop();  // Give up leadership temporarily.  Maybe another server will have more success.
        }
    }

    private boolean copyEvents() {
        // Use peek() not poll() since LeaderSelector ensures we're not competing with other processes for claims.
        List<EventData> rawEvents = _eventSource.get(1000);

        // If no events, sleep a little while before doing any more work to allow new events to arrive.
        if (rawEvents.isEmpty()) {
            return false;
        }

        // Last chance to check that we are the leader before doing anything that would be bad if we aren't.
        return isRunning() && copyEvents(rawEvents);
    }

    @VisibleForTesting
    boolean copyEvents(List<EventData> rawEvents) {
            // Read the list of subscriptions *after* reading events from the event store to avoid race conditions with
        // creating a new subscription.
        Iterable<OwnedSubscription> subscriptions = _subscriptionsSupplier.get();

        // Copy the events to all the destination channels.
        List<String> eventKeys = Lists.newArrayListWithCapacity(rawEvents.size());
        ListMultimap<String, ByteBuffer> eventsByChannel = ArrayListMultimap.create();
        int numOutboundReplicationEvents = 0;
        for (EventData rawEvent : rawEvents) {
            eventKeys.add(rawEvent.getId());

            ByteBuffer eventData = rawEvent.getData();

            SubscriptionEvaluator.MatchEventData matchEventData;
            try {
                matchEventData = _subscriptionEvaluator.getMatchEventData(eventData);
            } catch (UnknownTableException e) {
                continue;
            }

            // Copy to subscriptions in the current data center.
            for (OwnedSubscription subscription : _subscriptionEvaluator.matches(subscriptions, matchEventData)) {
                eventsByChannel.put(subscription.getName(), eventData);
            }

            // Copy to queues for eventual delivery to remote data centers.
            if (_replicateOutbound) {
                for (DataCenter dataCenter : matchEventData.getTable().getDataCenters()) {
                    if (!dataCenter.equals(_currentDataCenter)) {
                        String channel = ChannelNames.getReplicationFanoutChannel(dataCenter);
                        eventsByChannel.put(channel, eventData);
                        numOutboundReplicationEvents++;
                    }
                }
            }

            // Flush to cap the amount of memory used to buffer events.
            if (eventsByChannel.size() >= FLUSH_EVENTS_THRESHOLD) {
                flush(eventKeys, eventsByChannel, numOutboundReplicationEvents);
                numOutboundReplicationEvents = 0;
            }
        }

        // Final flush.
        flush(eventKeys, eventsByChannel, numOutboundReplicationEvents);

        return true;
    }

    private void flush(List<String> eventKeys, Multimap<String, ByteBuffer> eventsByChannel,
                       int numOutboundReplicationEvents) {
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
