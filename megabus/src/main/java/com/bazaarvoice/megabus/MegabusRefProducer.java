package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLog;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLogFactory;
import com.bazaarvoice.emodb.common.dropwizard.metrics.MetricsGroup;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.core.DatabusChannelConfiguration;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.bazaarvoice.emodb.databus.core.UpdateRefSerializer;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class MegabusRefProducer extends AbstractScheduledService {
    private final Logger _log;

    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);
    private static final int EVENTS_LIMIT = 4000;

    private final RateLimitedLog _rateLimitedLog;
    private final Databus _databus;
    private final DatabusEventStore _eventStore;
    private final MetricsGroup _timers;
    private final String _timerName;
    private final String _subscriptionName;
    private final Condition _subscriptionCondition;
    private final ScheduledExecutorService _executor;
    private final Producer<String, JsonNode> _producer;
    private final ObjectMapper _objectMapper;
    private final Topic _topic;

    public MegabusRefProducer(Databus databus, DatabusEventStore eventStore, Condition subscriptionCondition,
                              RateLimitedLogFactory logFactory, MetricRegistry metricRegistry,
                              Producer<String, JsonNode> producer, ObjectMapper objectMapper, Topic topic,
                              String instanceIdentifier) {
        this(databus, eventStore, subscriptionCondition, logFactory, metricRegistry, null, producer, objectMapper,
                topic, instanceIdentifier);
    }

    @VisibleForTesting
    MegabusRefProducer(Databus databus, DatabusEventStore eventStore, Condition subscriptionCondition,
                       RateLimitedLogFactory logFactory, MetricRegistry metricRegistry,
                       @Nullable ScheduledExecutorService executor, Producer<String, JsonNode> producer,
                       ObjectMapper objectMapper, Topic topic, String instanceIdentifier) {

        _log = LoggerFactory.getLogger(MegabusRefProducer.class.getName() + "-" + instanceIdentifier);
        _databus = checkNotNull(databus, "databus");
        _eventStore = checkNotNull(eventStore, "eventStore");
        _timers = new MetricsGroup(metricRegistry);
        _timerName = newTimerName("megabusPoll-" + instanceIdentifier);
        _rateLimitedLog = logFactory.from(_log);
        _executor = executor;
        _producer = checkNotNull(producer, "producer");
        _subscriptionCondition = checkNotNull(subscriptionCondition);

        //TODO : this is currently hacked to use instance identifiers to avoid dedup queues, which require the leader for consistent polling.
        // We should ideally make the megabus poller also the dedup leader, which should allow consistent polling and deduping, which should cluster updates to the same key
        _subscriptionName = "__system_bus:" + "megabus-" + instanceIdentifier;
        _objectMapper = checkNotNull(objectMapper, "objectMapper");
        _topic = checkNotNull(topic, "topic");
        createMegabusSubscription();
        ServiceFailureListener.listenTo(this, metricRegistry);


    }

    private String newTimerName(String name) {
        return MetricRegistry.name("bv.emodb.databus", "Megabus", name, "readEvents");
    }

    private void createMegabusSubscription() {
        // Except for resetting the ttl, recreating a subscription that already exists has no effect.
        // Assume that multiple servers that manage the same subscriptions can each attempt to create
        // the subscription at startup.  The subscription should last basically forever.
        // Note: make sure that we don't ignore any events
        _databus.subscribe(_subscriptionName, _subscriptionCondition,
                DatabusChannelConfiguration.CANARY_TTL, DatabusChannelConfiguration.CANARY_TTL, false);
    }

    @Override
    protected AbstractScheduledService.Scheduler scheduler() {
        return AbstractScheduledService.Scheduler.newFixedDelaySchedule(0, POLL_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void shutDown() throws Exception {
        _timers.close();  // Lost leadership.  Stop reporting metrics so we don't conflict with the new leader.
    }

    @Override
    protected ScheduledExecutorService executor() {
        // If an explicit executor was provided use it, otherwise create a default executor
        return _executor != null ? _executor : super.executor();
    }

    @Override
    protected void runOneIteration() {
        try {
            //noinspection StatementWithEmptyBody
            while (isRunning() && pollAndAckEvents()) {
                // Loop w/o sleeping as long as we keep finding events
            }
        } catch (Throwable t) {
            _rateLimitedLog.error(t, "Unexpected megabus exception: {}", t);
            stop();  // Give up leadership temporarily.  Maybe another server will have more success.
        }
    }

    public static class Event {
        public String id;
        public UpdateRef payload;  // Name matches the QueueService Message object.

        private Event(String id, UpdateRef payload) {
            this.id = id;
            this.payload = payload;
        }
    }

    private boolean pollAndAckEvents() {
        // Poll for events on the megabus subscription
        long startTime = System.nanoTime();
        List<String> eventKeys = Lists.newArrayList();
        List<EventData> result = _eventStore.peek(_subscriptionName, EVENTS_LIMIT);
        List<Event> events = Lists.transform(result, event -> new Event(event.getId(), UpdateRefSerializer.fromByteBuffer(event.getData())));
        Multimap<Integer, UpdateRef> refsByPartition = ArrayListMultimap.create(_topic.getPartitions(), EVENTS_LIMIT / _topic.getPartitions());
        for (Event event : events) {
            String key = Coordinate.of(event.payload.getTable(), event.payload.getKey()).toString();
            refsByPartition.put(Utils.toPositive(Utils.murmur2(key.getBytes())) % _topic.getPartitions(), event.payload);
            eventKeys.add(event.id);
        }

        List<Future> futures = new ArrayList<>();

        refsByPartition.keySet().forEach(key -> {
            futures.add(_producer.send(new ProducerRecord<>(_topic.getName(), key,
                    TimeUUIDs.newUUID().toString(), _objectMapper.valueToTree(refsByPartition.get(key)))));
        });

        long endTime = System.nanoTime();
        trackAverageEventDuration(endTime - startTime, eventKeys.size());

        // Last chance to check that we are the leader before doing anything that would be bad if we aren't.
        if (!isRunning()) {
            return false;
        }

        _producer.flush();

        futures.forEach(Futures::getUnchecked);

        // Ack these events
        if (!eventKeys.isEmpty()) {
            _databus.acknowledge(_subscriptionName, eventKeys);
        }

        return !eventKeys.isEmpty();
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
