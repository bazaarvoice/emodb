package com.bazaarvoice.megabus.resolver;

import com.bazaarvoice.emodb.kafka.JsonPOJOSerde;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.KafkaProducerConfiguration;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.table.db.test.InMemoryTable;
import com.bazaarvoice.megabus.MegabusRef;
import com.bazaarvoice.megabus.TestDataProvider;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.util.ISO8601Utils;
import com.google.common.net.HostAndPort;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import java.time.Clock;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class MegabusRefResolverTest {

    // ref resolver stuff
    private static final Topic refTopic = new Topic("refs", 1, (short) 1);
    private static final Topic resolvedTopic = new Topic("resolvedRefs", 1, (short) 1);
    private static final Topic retryTopic = new Topic("retry", 1, (short) 1);
    private static final Topic missingRefsTopic = new Topic("missingRefs", 1, (short) 1);
    private static final KafkaCluster kafkaCluster = mock(KafkaCluster.class);
    private static final DataProvider dataProvider = new TestDataProvider();
    private static final HostAndPort kafkaEndpoint = HostAndPort.fromParts("localhost", 9092);

    static {
        when(kafkaCluster.getBootstrapServers()).thenReturn(kafkaEndpoint.toString());
        when(kafkaCluster.getProducerConfiguration()).thenReturn(new KafkaProducerConfiguration());
    }

    private static final MegabusRefResolver refResolver = new MegabusRefResolver(
        dataProvider, refTopic, resolvedTopic, retryTopic, missingRefsTopic,
        kafkaCluster, Clock.systemUTC(), kafkaEndpoint,
        "refResolverGroup", new MetricRegistry()
    );

    // Kafka test driver stuff
    private TopologyTestDriver testDriver;

    private static final StringDeserializer stringDeserializer = new StringDeserializer();
    private static final JsonPOJOSerde<List<MegabusRef>> jsonPOJOSerdeForMegabusList = new JsonPOJOSerde<>(new TypeReference<List<MegabusRef>>() {});
    private static final JsonPOJOSerde<Map<String, Object>> jsonPOJOSerdeForMaps = new JsonPOJOSerde<>(new TypeReference<Map<String, Object>>() {});
    private static final JsonPOJOSerde<MissingRefCollection> jsonPOJOSerdeForMissingRefCollection = new JsonPOJOSerde<>(new TypeReference<MissingRefCollection>() {});
    private static final ConsumerRecordFactory<String, List<MegabusRef>> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), jsonPOJOSerdeForMegabusList.serializer());

    @BeforeSuite
    public void setUp() {
        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "refResolverGroup");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEndpoint.toString());
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        testDriver = new TopologyTestDriver(refResolver.topology(), config);
    }

    @Test
    public void testTopologyResolvingOneRef() {
        final String testTableName = "tableA";
        final String testId = "id1";
        final Map<String, Object> testContents = new HashMap<String, Object>() {{
            put("~table", testTableName);
            put("~id", testId);
            put("~version", 0);
            put("~signature", "abc123");
            put("~deleted", false);
            put("~firstUpdateAt", ISO8601Utils.format(new Date()));
            put("~lastUpdateAt", ISO8601Utils.format(new Date()));
            put("~lastMutateAt", ISO8601Utils.format(new Date()));
        }};

        MegabusRef megabusRef = new MegabusRef(testTableName, testId, TimeUUIDs.newUUID(), Instant.now(), MegabusRef.RefType.NORMAL);

        ((TestDataProvider) dataProvider).addTable(testTableName, new InMemoryTable(testTableName, new TableOptionsBuilder().setPlacement("app_global").build(), new HashMap<>()));
        ((TestDataProvider) dataProvider).add(testContents);

        List<MegabusRef> megabusRefs = Collections.singletonList(megabusRef);

        // push the megabusref to the input topic
        testDriver.pipeInput(recordFactory.create(refTopic.getName(), "eventId1", megabusRefs));

        // read the result
        final ProducerRecord<String, Map<String, Object>> output = testDriver.readOutput(resolvedTopic.getName(), stringDeserializer, jsonPOJOSerdeForMaps.deserializer());

        // ensure ref was resolved successfully and had the correct value and was written to the correct topic
        OutputVerifier.compareKeyValue(output,  String.format("%s/%s", testTableName, testId), testContents);

        // ensure no more records left unread
        assertNull(testDriver.readOutput(resolvedTopic.getName(), stringDeserializer, jsonPOJOSerdeForMaps.deserializer()));
    }

    @Test
    public void testTopologyMissingOneRef() {
        final String testTableName = "tableA";
        final String testId = "id1";
        final Map<String, Object> testContents = new HashMap<String, Object>() {{
            put("~table", testTableName);
            put("~id", testId);
            put("~version", 0);
            put("~signature", "abc123");
            put("~deleted", false);
            put("~firstUpdateAt", ISO8601Utils.format(new Date()));
            put("~lastUpdateAt", ISO8601Utils.format(new Date()));
            put("~lastMutateAt", ISO8601Utils.format(new Date()));
        }};

        MegabusRef megabusRef = new MegabusRef(testTableName, testId, TimeUUIDs.newUUID(), Instant.now(), MegabusRef.RefType.NORMAL);

        ((TestDataProvider) dataProvider).addTable(testTableName, new InMemoryTable(testTableName, new TableOptionsBuilder().setPlacement("app_global").build(), new HashMap<>()));
        ((TestDataProvider) dataProvider).addPending(testContents);

        List<MegabusRef> megabusRefs = Collections.singletonList(megabusRef);

        // push the megabusref to the input topic
        testDriver.pipeInput(recordFactory.create(refTopic.getName(), "eventId1", megabusRefs));

        // ensure the resolved topic does _NOT_ have the ref (it should not have resolved)
        assertNull(testDriver.readOutput(resolvedTopic.getName(), stringDeserializer, jsonPOJOSerdeForMaps.deserializer()));

        // read the result  from the "missing" topic
        final ProducerRecord<String, MissingRefCollection> output = testDriver.readOutput(missingRefsTopic.getName(), stringDeserializer, jsonPOJOSerdeForMissingRefCollection.deserializer());

        // ensure ref was not resolved successfully and had the correct key, and had exactly the missing reference and was written to the correct topic
        assertEquals(output.key(), "eventId1");
        assertEquals(output.value().getMissingRefs(), Collections.singletonList(megabusRef));

        // ensure no more records left unread in the missing refs topic
        assertNull(testDriver.readOutput(missingRefsTopic.getName(), stringDeserializer, jsonPOJOSerdeForMaps.deserializer()));
    }

    @Test
    public void testTopologyMergesRetries() {
        final String testTableName = "tableA";
        final String testId = "id1";
        final Map<String, Object> testContents = new HashMap<String, Object>() {{
            put("~table", testTableName);
            put("~id", testId);
            put("~version", 0);
            put("~signature", "abc123");
            put("~deleted", false);
            put("~firstUpdateAt", ISO8601Utils.format(new Date()));
            put("~lastUpdateAt", ISO8601Utils.format(new Date()));
            put("~lastMutateAt", ISO8601Utils.format(new Date()));
        }};

        MegabusRef megabusRef = new MegabusRef(testTableName, testId, TimeUUIDs.newUUID(), Instant.now(), MegabusRef.RefType.NORMAL);

        ((TestDataProvider) dataProvider).addTable(testTableName, new InMemoryTable(testTableName, new TableOptionsBuilder().setPlacement("app_global").build(), new HashMap<>()));
        ((TestDataProvider) dataProvider).add(testContents);

        List<MegabusRef> megabusRefs = Collections.singletonList(megabusRef);

        // push the megabusref to the input topic
        testDriver.pipeInput(recordFactory.create(retryTopic.getName(), "eventId1", megabusRefs));

        // read the result
        final ProducerRecord<String, Map<String, Object>> output = testDriver.readOutput(resolvedTopic.getName(), stringDeserializer, jsonPOJOSerdeForMaps.deserializer());

        // ensure ref was resolved successfully and had the correct value and was written to the correct topic
        OutputVerifier.compareKeyValue(output,  String.format("%s/%s", testTableName, testId), testContents);

        // ensure no more records left unread
        assertNull(testDriver.readOutput(resolvedTopic.getName(), stringDeserializer, jsonPOJOSerdeForMaps.deserializer()));
    }

    @AfterSuite
    public void tearDown() {
        testDriver.close();
    }

}
