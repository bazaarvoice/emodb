package com.bazaarvoice.megabus;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLogFactory;
import com.bazaarvoice.emodb.common.zookeeper.leader.PartitionedLeaderService;
import com.bazaarvoice.emodb.common.zookeeper.leader.PartitionedServiceSupplier;
import com.bazaarvoice.emodb.databus.DatabusOstrichOwnerGroupFactory;
import com.bazaarvoice.emodb.databus.SystemIdentity;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.bazaarvoice.emodb.databus.core.DatabusFactory;
import com.bazaarvoice.emodb.event.owner.OstrichOwnerGroupFactory;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import java.time.Clock;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

public class MegabusRefProducerManager {

    private static final int MAX_PUBLISH_RETRIES = 2;
    private static final String ACKS_CONFIG = "all";

    private static final int NUM_PARTITIONS = 8;

    @Inject
    public MegabusRefProducerManager(final LifeCycleRegistry lifeCycle,
                                     LeaderServiceTask leaderServiceTask,
                                     @MegabusZookeeper CuratorFramework curator,
                                     @MegabusRefTopic Topic refTopic,
                                     @SelfHostAndPort HostAndPort hostAndPort,
                                     Clock clock,
                                     final DatabusFactory databusFactory,
                                     DatabusEventStore databusEventStore,
                                     final @SystemIdentity String systemId,
                                     final RateLimitedLogFactory logFactory,
                                     @DatabusOstrichOwnerGroupFactory OstrichOwnerGroupFactory ownerGroupFactory,
                                     final MetricRegistry metricRegistry,
                                     KafkaCluster kafkaCluster,
                                     ObjectMapper objectMapper) {

        kafkaCluster.createTopicIfNotExists(refTopic);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG);
        props.put(ProducerConfig.RETRIES_CONFIG, MAX_PUBLISH_RETRIES);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, "megabus-producer");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        Producer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(props);

        // TODO: since partitioned databus subscriptions are 1-based, we must add one to the partition condition. At some point in the future,
        // we should reconcile this inconsistency
        PartitionedServiceSupplier refProducerSupplier = partition ->
                new MegabusRefProducer(databusFactory.forOwner(systemId), databusEventStore,
                        Conditions.partition(NUM_PARTITIONS, partition + 1),
                        logFactory, metricRegistry, producer, objectMapper, refTopic, Integer.toString(partition));

        PartitionedLeaderService partitionedLeaderService = new PartitionedLeaderService(
                curator, ZKPaths.makePath("leader", "partitioned-megabus-ref-producer"), hostAndPort.toString(),
                "PartitionedLeaderSelector-megabusRefProducer", NUM_PARTITIONS, 1, 1, TimeUnit.MINUTES,
                refProducerSupplier, clock);

        for (LeaderService leaderService : partitionedLeaderService.getPartitionLeaderServices()) {
            ServiceFailureListener.listenTo(leaderService, metricRegistry);
        }

        leaderServiceTask.register("megabus-ref-producer", partitionedLeaderService);
        lifeCycle.manage(partitionedLeaderService);

        // Since the megabus reads from the databus it must either (a) execute on the same server that owns
        // and manages the databus megabus subscription or (b) go through the Ostrich client that forwards
        // requests to the right server.  This code implements the first option by using the same consistent
        // hash calculation used by Ostrich to determine which server owns the megabus subscription.  As Emo
        // servers join and leave the pool, the OwnerGroup will track which server owns the megabus subscription
        // at a given point in time and start and stop the megabus service appropriately.
//        OwnerGroup<Service> ownerGroup = ownerGroupFactory.create("Megabus", new OstrichOwnerFactory<Service>() {
//            @Override
//            public PartitionContext getContext(String partition) {
//                return PartitionContextBuilder.of("Megabus-" + partition);
//            }
//
//            @Override
//            public Service create(String partition) {
//                Databus databus = databusFactory.forOwner(systemId);
//
//                return new MegabusRefProducer(databus, databusEventStore, Conditions.partition(NUM_PARTITIONS, Integer.parseInt(partition)),
//                        logFactory, metricRegistry, producer, objectMapper, refTopic, partition);
//            }
//        }, null);
//        lifeCycle.manage(ownerGroup);
//
//        // Start one megabus for each partition
//        for (int i = 1; i <= NUM_PARTITIONS; i++) {
//            ownerGroup.startIfOwner(Integer.toString(i), Duration.ZERO);
//        }
    }

}