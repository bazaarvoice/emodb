package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLogFactory;
import com.bazaarvoice.emodb.databus.SystemIdentity;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.event.owner.OstrichOwnerFactory;
import com.bazaarvoice.emodb.event.owner.OstrichOwnerGroupFactory;
import com.bazaarvoice.emodb.event.owner.OwnerGroup;
import com.bazaarvoice.ostrich.PartitionContext;
import com.bazaarvoice.ostrich.PartitionContextBuilder;
import com.codahale.metrics.MetricRegistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import java.time.Duration;

import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

public class MegaBusManager {

    private static final int MAX_PUBLISH_RETRIES = 2;
    private static final String ACKS_CONFIG = "all";

    private final static int NUM_PARTITIONS = 8;
    private final static String TOPIC_NAME = "megabus";

    @Inject
    public MegaBusManager(final LifeCycleRegistry lifeCycle,
                          final DatabusFactory databusFactory,
                          DatabusEventStore databusEventStore,
                          final @SystemIdentity String systemId,
                          final RateLimitedLogFactory logFactory,
                          OstrichOwnerGroupFactory ownerGroupFactory,
                          final MetricRegistry metricRegistry,
                          ObjectMapper objectMapper) {

        createMegabusTopic();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG);
        props.put(ProducerConfig.RETRIES_CONFIG, MAX_PUBLISH_RETRIES);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, "megabus-producer");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        Producer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(props);


        // Since the megabus reads from the databus it must either (a) execute on the same server that owns
        // and manages the databus megabus subscription or (b) go through the Ostrich client that forwards
        // requests to the right server.  This code implements the first option by using the same consistent
        // hash calculation used by Ostrich to determine which server owns the megabus subscription.  As Emo
        // servers join and leave the pool, the OwnerGroup will track which server owns the megabus subscription
        // at a given point in time and start and stop the megabus service appropriately.
        OwnerGroup<Service> ownerGroup = ownerGroupFactory.create("Megabus", new OstrichOwnerFactory<Service>() {
            @Override
            public PartitionContext getContext(String partition) {
                return PartitionContextBuilder.of("Megabus-" + partition);
            }

            @Override
            public Service create(String partition) {
                Databus databus = databusFactory.forOwner(systemId);

                return new Megabus(Integer.parseInt(partition), NUM_PARTITIONS, databus, databusEventStore, logFactory, metricRegistry,
                        producer, objectMapper, TOPIC_NAME);
            }
        }, null);
        lifeCycle.manage(ownerGroup);

        // Start one megabus for each partition
        for (int i = 1; i <= NUM_PARTITIONS; i++) {
            ownerGroup.startIfOwner(Integer.toString(i), Duration.ZERO);
        }
    }

    private void createMegabusTopic() {

        scala.Tuple2 zkClientAndConnection = ZkUtils.createZkClientAndConnection("localhost:2181", 30000, 30000);
        ZkUtils zkUtils =  new ZkUtils((ZkClient)zkClientAndConnection._1, (ZkConnection)zkClientAndConnection._2, false);

        // Explicitly create topics
        if (!AdminUtils.topicExists(zkUtils, TOPIC_NAME)) {
            AdminUtils.createTopic(zkUtils, TOPIC_NAME, NUM_PARTITIONS, 1, new Properties(), RackAwareMode.Safe$.MODULE$);
        }

    }
}
