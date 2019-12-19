package com.bazaarvoice.megabus.refproducer;

import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLogFactory;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.bazaarvoice.emodb.databus.core.UpdateRefSerializer;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.bazaarvoice.megabus.MegabusRef;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class MegabusRefProducerTest {

    private static final Producer<String, JsonNode> producer = mock(Producer.class);
    private static final DatabusEventStore databus = mock(DatabusEventStore.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Clock clock = mock(Clock.class);
    private static final MegabusRefProducerConfiguration config = new MegabusRefProducerConfiguration();
    private static final String subname = "subscription";
    private static final Integer partitionId = 5;
    private static final Topic topic = new Topic("mytopic", 16, (short) 1);
    private static final Instant staticInstant = Instant.now();

    private static final MegabusRefProducer INSTANCE = new MegabusRefProducer(config, databus,
        mock(RateLimitedLogFactory.class), mock(MetricRegistry.class), null, producer,
        objectMapper, topic, subname, "1", clock);

    @Test
    public void testPeekAndAckEvents() {

        final String tableName = "tableA";
        final String documentId = "id1";
        final UUID documentUuid = UUID.randomUUID();
        final UpdateRef ref = new UpdateRef(tableName, documentId, documentUuid, new HashSet<>());
        final List<EventData> singleResult = Collections.singletonList(new TestEventData("ev1", UpdateRefSerializer.toByteBuffer(ref)));

        when(databus.peek(subname, config.getBatchSize())).thenReturn(singleResult);
        when(clock.instant()).thenReturn(staticInstant);

        // it'd have been better to just go with INSTANCE#runOneIteration, but that requires INSTANCE#isRunning to be true
        // which requires _actually running the service_ and makes testing less precise.
        INSTANCE.peekAndAckEvents();

        ArgumentCaptor<ProducerRecord> argument = ArgumentCaptor.forClass(ProducerRecord.class);

        verify(producer).send(argument.capture());
        assertEquals(argument.getValue().value(), objectMapper.valueToTree(Collections.singletonList((new MegabusRef(tableName, documentId, documentUuid, staticInstant, false)))) );
        assertEquals(argument.getValue().partition(), partitionId);
        assertEquals(argument.getValue().topic(), topic.getName());

        // this doesn't work because Matchers reference a static type (null) and ProducerRecord checks to ensure topic is not null
        // ergo, ProducerRecord itself must be matched against
        // this means the only way to verify that the values are as expected is using an ArgumentCaptor or a custom ArgumentMatcher
        // ArgumentCaptor is more convenient here
        // note also that I cannot use Matcher.any() for the TimeUUID, as I cannot mix Matchers and literal arguments, and
        // the Matcher for the topic name is represented as null, and...well, see the first line of this comment
//        verify(producer).send(new ProducerRecord<>(topic.getName(), partitionId, documentUuid.toString(),
//            objectMapper.valueToTree(new MegabusRef(tableName, documentId, documentUuid, staticInstant))));

    }

    private class TestEventData implements EventData {
        private final String _id;
        private final ByteBuffer _data;

        TestEventData(String id, ByteBuffer data) {
            _id = Objects.requireNonNull(id, "id");
            _data = Objects.requireNonNull(data, "data");
        }

        @Override
        public String getId() {
            return _id;
        }

        @Override
        public ByteBuffer getData() {
            return _data.duplicate();
        }
    }

}
