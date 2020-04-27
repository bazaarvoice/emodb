package com.bazaarvoice.megabus.tableevents;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.table.db.eventregistry.TableEventTools;
import com.bazaarvoice.emodb.table.db.eventregistry.TableEvent;
import com.bazaarvoice.emodb.table.db.eventregistry.TableEventRegistry;
import com.bazaarvoice.megabus.MegabusRef;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Futures;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * this is the leader elected process to send the READY table events to Megabus Ref topic, and then updating the events
 * as complete in the system table.
 */
public class TableEventProcessor extends AbstractScheduledService {

    private static final int FUTURE_BATCH_SIZE = 10000;

    private static final Logger _log = LoggerFactory.getLogger(TableEventProcessor.class);

    private final TableEventRegistry _tableEventRegistry;
    private final MetricRegistry _metricRegistry;
    private final String _applicationId;
    private final TableEventTools _tableEventTools;
    private final Producer<String, JsonNode> _producer;
    private final Topic _topic;
    private final ObjectMapper _objectMapper;

    public TableEventProcessor(String applicationId,
                               TableEventRegistry tableEventRegistry,
                               MetricRegistry metricRegistry,
                               TableEventTools tableEventTools,
                               Producer<String, JsonNode> producer,
                               ObjectMapper objectMapper,
                               Topic topic) {
        _tableEventRegistry = requireNonNull(tableEventRegistry);
        _metricRegistry = requireNonNull(metricRegistry);
        _applicationId = requireNonNull(applicationId);
        _tableEventTools = requireNonNull(tableEventTools);
        _producer = requireNonNull(producer);
        _objectMapper = requireNonNull(objectMapper);
        _topic = requireNonNull(topic);
    }

    @Override
    protected void runOneIteration() throws Exception {
        Map.Entry<String, TableEvent> tableEventPair;

        while ((tableEventPair = _tableEventRegistry.getNextReadyTableEvent(_applicationId)) != null) {

            String table = tableEventPair.getKey();
            TableEvent tableEvent = tableEventPair.getValue();

            processTableEvent(table, tableEvent.getStorage(), tableEvent.getAction() == TableEvent.Action.DROP);

            _tableEventRegistry.markTableEventAsComplete(_applicationId, table, tableEvent.getStorage());
        }
    }

    private void processTableEvent(String table, String uuid, boolean deleted) {

        Iterator<Future<RecordMetadata>> futures =  _tableEventTools.getIdsForStorage(table, uuid)
                .map(key -> new MegabusRef(table, key, TimeUUIDs.minimumUuid(), null, deleted))
                .map(ref -> {
                    String key = Coordinate.of(ref.getTable(), ref.getKey()).toString();
                    return new ProducerRecord<String, JsonNode>(_topic.getName(),
                            Utils.toPositive(Utils.murmur2(key.getBytes())) % _topic.getPartitions(),
                            TimeUUIDs.newUUID().toString(),_objectMapper.valueToTree(Collections.singletonList(ref)));
                })
                .map(_producer::send)
                .iterator();

        Iterator<List<Future<RecordMetadata>>> partitionedFutures = Iterators.partition(futures, FUTURE_BATCH_SIZE);

        while (partitionedFutures.hasNext()) {
            partitionedFutures.next().forEach(Futures::getUnchecked);
        }
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(0, 1, TimeUnit.HOURS);
    }
}
