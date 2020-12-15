package com.bazaarvoice.megabus.refproducer;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLog;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLogFactory;
import com.bazaarvoice.emodb.common.dropwizard.metrics.MetricsGroup;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.bazaarvoice.emodb.databus.core.UpdateRefSerializer;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.megabus.MegabusRef;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class MegabusRefProducer extends AbstractScheduledService {
    private final Logger _log;


    private final int _pollIntervalMs;
    private final int _eventsLimit;
    private final int _skipWaitThreshold;

    private final RateLimitedLog _rateLimitedLog;
    private final DatabusEventStore _eventStore;
    private final MetricsGroup _timers;
    private final String _timerName;
    private final String _subscriptionName;
    private final ScheduledExecutorService _executor;
    private final Producer<String, JsonNode> _producer;
    private final ObjectMapper _objectMapper;
    private final Topic _topic;
    private final Clock _clock;

    private final Meter _eventMeter;
    private final Meter _errorMeter;

    public MegabusRefProducer(MegabusRefProducerConfiguration config, DatabusEventStore eventStore,
                              RateLimitedLogFactory logFactory, MetricRegistry metricRegistry,
                              Producer<String, JsonNode> producer, ObjectMapper objectMapper, Topic topic,
                              String subscriptionName, String partitionIdentifier) {
        this(config, eventStore, logFactory, metricRegistry, null, producer, objectMapper,
                topic, subscriptionName, partitionIdentifier, null);
    }

    @VisibleForTesting
    MegabusRefProducer(MegabusRefProducerConfiguration configuration, DatabusEventStore eventStore,
                       RateLimitedLogFactory logFactory, MetricRegistry metricRegistry,
                       @Nullable ScheduledExecutorService executor, Producer<String, JsonNode> producer,
                       ObjectMapper objectMapper, Topic topic, String subscriptionName,
                       String partitionIdentifer, Clock clock) {

        _log = LoggerFactory.getLogger(MegabusRefProducer.class.getName() + "-" + partitionIdentifer);

        if (configuration.getPollIntervalMs() <= 0) {
            throw new IllegalArgumentException("Poll interval must be >0");
        }
        if (configuration.getBatchSize() <= 0) {
            throw new IllegalArgumentException("Batch size must be >0");
        }
        if (configuration.getSkipWaitThreshold() < 0 || configuration.getSkipWaitThreshold() > configuration.getBatchSize()) {
            throw new IllegalArgumentException("Skip wait threshold must be >=0 and <= batch size=" + configuration.getBatchSize());
        }
        _pollIntervalMs = configuration.getPollIntervalMs();
        _eventsLimit = configuration.getBatchSize();
        _skipWaitThreshold = configuration.getSkipWaitThreshold();
        _eventStore = requireNonNull(eventStore, "eventStore");
        _timers = new MetricsGroup(metricRegistry);
        _timerName = newTimerName("megabusPoll-" + partitionIdentifer);
        _rateLimitedLog = logFactory.from(_log);
        _executor = executor;
        _producer = requireNonNull(producer, "producer");
        _clock = Optional.ofNullable(clock).orElse(Clock.systemUTC());
        _eventMeter = metricRegistry.meter(MetricRegistry.name("bv.emodb.megabus", "MegabusRefProducer", "events"));
        _errorMeter = metricRegistry.meter(MetricRegistry.name("bv.emodb.megabus", "MegabusRefProducer", "errors"));

        // TODO: We should ideally make the megabus poller also the dedup leader, which should allow consistent polling and deduping, as well as cluster updates to the same key
        // NOTE: megabus subscriptions currently avoid dedup queues by starting with "__"
        _subscriptionName = requireNonNull(subscriptionName, "subscriptionName");
        _objectMapper = requireNonNull(objectMapper, "objectMapper");
        _topic = requireNonNull(topic, "topic");
        ServiceFailureListener.listenTo(this, metricRegistry);

    }

    private String newTimerName(String name) {
        return MetricRegistry.name("bv.emodb.megabus", name, "readEvents");
    }

    @Override
    protected AbstractScheduledService.Scheduler scheduler() {
        return AbstractScheduledService.Scheduler.newFixedDelaySchedule(0, _pollIntervalMs, TimeUnit.MILLISECONDS);
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
            while (isRunning() && peekAndAckEvents()) {
                // Loop w/o sleeping as long as we keep finding events
            }
        } catch (Throwable t) {
            _rateLimitedLog.error(t, "Unexpected megabus exception: {}", t);
            _errorMeter.mark();
            // Give up leadership temporarily.  Maybe another server will have more success.
            stopAsync();
        }
    }

    @VisibleForTesting
    boolean peekAndAckEvents() {
        // Poll for events on the megabus subscription
        long startTime = _clock.instant().getNano();
        List<EventData> result = _eventStore.peek(_subscriptionName, _eventsLimit);

        List<Future> futures = result.stream()
                .map(eventData -> UpdateRefSerializer.fromByteBuffer(eventData.getData()))
                .map(ref -> new MegabusRef(ref.getTable(), ref.getKey(), ref.getChangeId(), _clock.instant(), MegabusRef.RefType.NORMAL))
                .collect(Collectors.groupingBy(ref -> {
                    String key = Coordinate.of(ref.getTable(), ref.getKey()).toString();
                    return Utils.toPositive(Utils.murmur2(key.getBytes())) % _topic.getPartitions();
                }, Collectors.toList()))
                .entrySet()
                .stream()
                .map(entry -> _producer.send(new ProducerRecord<>(_topic.getName(), entry.getKey(), TimeUUIDs.newUUID().toString(),
                        _objectMapper.valueToTree(entry.getValue()))))
                .collect(Collectors.toList());

        // Last chance to check that we are the leader before doing anything that would be bad if we aren't.
        if (!isRunning()) {
            return false;
        }

        _producer.flush();

        futures.forEach(Futures::getUnchecked);

        long endTime = _clock.instant().getNano();
        trackAverageEventDuration(endTime - startTime, result.size());

        if (!result.isEmpty()) {
            _eventStore.delete(_subscriptionName, result.stream().map(EventData::getId).collect(Collectors.toList()), false);
        }

        return result.size() >= _skipWaitThreshold;
    }

    private void trackAverageEventDuration(long durationInNs, int numEvents) {
        if (numEvents == 0) {
            return;
        }

        _eventMeter.mark(numEvents);

        long durationPerEvent = (durationInNs + numEvents - 1) / numEvents;  // round up

        _timers.beginUpdates();
        Timer timer = _timers.timer(_timerName, TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS);
        for (int i = 0; i < numEvents; i++) {
            timer.update(durationPerEvent, TimeUnit.NANOSECONDS);
        }
        _timers.endUpdates();
    }
}
