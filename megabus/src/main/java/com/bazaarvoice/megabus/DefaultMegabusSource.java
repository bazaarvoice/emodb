package com.bazaarvoice.megabus;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.megabus.guice.MegabusRefTopic;
import com.bazaarvoice.megabus.resource.Coordinate;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Default implementation which uses the existing Megabus actors.
 */
public class DefaultMegabusSource implements MegabusSource {

    private static final Logger _LOG = LoggerFactory.getLogger(DefaultMegabusSource.class);

    private final Producer<String, JsonNode> _producer;
    private final Topic _topic;
    private final ObjectMapper _objectMapper;
    private final Clock _clock;

    @Inject
    public DefaultMegabusSource(KafkaCluster kafkaCluster, @MegabusRefTopic Topic topic,
                                ObjectMapper objectMapper, Clock clock) {
        this(kafkaCluster.producer(), topic, objectMapper, clock);
    }

    @VisibleForTesting
    public DefaultMegabusSource(Producer<String, JsonNode> producer, Topic topic,
                                ObjectMapper objectMapper, Clock clock) {
        _producer = requireNonNull(producer, "producer");
        _topic = requireNonNull(topic, "topic");
        _objectMapper = requireNonNull(objectMapper, "objectMapper");
        _clock = requireNonNull(clock, "clock");
    }

    /*
     * Send the given co-ordinate to the Megabus Ref Topic.
     */
    @Override
    public void touch(Coordinate coordinate) {
        touchAll(Collections.singletonList(coordinate).iterator());
    }

    /*
     * Send the given input refs to the Megabus Ref Topic.
     */
    @Override
    public void touchAll(Iterator<Coordinate> coordinates) {
        List<Coordinate> coordinateList = Lists.newArrayList();
        coordinates.forEachRemaining(coordinateList::add);

        _LOG.info("Sending {} coordinate(s) to Megabus Ref Topic: {}", coordinateList.size(), _topic.getName());
        List<Future> futures = getSendFutures(coordinateList);
        _producer.flush();
        futures.forEach(Futures::getUnchecked);
    }

    @VisibleForTesting
    public List<Future> getSendFutures(List<Coordinate> coordinateList) {
        List<Future> futures = coordinateList
                .stream()
                // Using the minimum UUID here to make sure the time is always beyond the FCL so that the resolver is certain to put the document in to actual Megabus.
                // This way we wouldn't be in a situation where there is a ref in Ref topic but not in the Megabus topic.
                .map(coordinate -> new MegabusRef(coordinate.getTable(), coordinate.getKey(), TimeUUIDs.minimumUuid(), _clock.instant()))
                .collect(Collectors.groupingBy(ref -> {
                    String key = Coordinate.of(ref.getTable(), ref.getKey()).toString();
                    return Utils.toPositive(Utils.murmur2(key.getBytes())) % _topic.getPartitions();
                }, Collectors.toList()))
                .entrySet()
                .stream()
                .map(entry -> _producer.send(new ProducerRecord<>(_topic.getName(), entry.getKey(), TimeUUIDs.newUUID().toString(),
                        _objectMapper.valueToTree(entry.getValue()))))
                .collect(Collectors.toList());

        return futures;
    }
}
