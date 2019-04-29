package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLog;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLogFactory;
import com.bazaarvoice.emodb.common.dropwizard.metrics.MetricsGroup;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.event.api.EventData;
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
import java.time.Duration;
import java.util.List;
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

public class Megabus extends AbstractScheduledService {
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
    private final String _producerTopicName;

    public Megabus(int partition, int totalPartitions, Databus databus, DatabusEventStore eventStore, RateLimitedLogFactory logFactory,
                   MetricRegistry metricRegistry, Producer<String, JsonNode> producer, ObjectMapper objectMapper,
                   String producerTopicName) {
        this(partition, totalPartitions, databus, eventStore, logFactory, metricRegistry, null, producer, objectMapper, producerTopicName);
    }

    @VisibleForTesting
    Megabus(int partition, int totalPartitions, Databus databus, DatabusEventStore eventStore, RateLimitedLogFactory logFactory,
            MetricRegistry metricRegistry, @Nullable ScheduledExecutorService executor,
            Producer<String, JsonNode> producer, ObjectMapper objectMapper, String producerTopicName) {
        checkArgument(totalPartitions > 0);
        checkArgument(partition > 0);
        checkArgument(partition <= totalPartitions);

        _log = LoggerFactory.getLogger(Megabus.class.getName() + "-" + partition);
        _databus = checkNotNull(databus, "databus");
        _eventStore = checkNotNull(eventStore, "eventStore");
        _timers = new MetricsGroup(metricRegistry);
        _timerName = newTimerName("megabusPoll-" + partition);
        _rateLimitedLog = logFactory.from(_log);
        _executor = executor;
        _producer = checkNotNull(producer, "producer");
        _subscriptionCondition = Conditions.partition(totalPartitions, partition);
        _subscriptionName = "megabus-" + partition;
        _objectMapper = checkNotNull(objectMapper, "objectMapper");
        _producerTopicName = checkNotNull(producerTopicName, "producerTopicName");
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
                Duration.ofDays(3650), DatabusChannelConfiguration.CANARY_TTL, false);
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
        Multimap<Integer, UpdateRef> refsByPartition = ArrayListMultimap.create(8, EVENTS_LIMIT / 8);
        for (Event event : events) {
            String key = event.payload.getKey().concat("/").concat(event.payload.getTable());
            refsByPartition.put(Utils.toPositive(Utils.murmur2(key.getBytes())) % 8, event.payload);
            eventKeys.add(event.id);
        }

        refsByPartition.keySet().forEach(key -> {
            _producer.send(new ProducerRecord<>(_producerTopicName, key,
                    TimeUUIDs.newUUID().toString(), _objectMapper.valueToTree(refsByPartition.get(key))));
        });

        long endTime = System.nanoTime();
        trackAverageEventDuration(endTime - startTime, eventKeys.size());

        // Last chance to check that we are the leader before doing anything that would be bad if we aren't.
        if (!isRunning()) {
            return false;
        }

        _producer.flush();

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
