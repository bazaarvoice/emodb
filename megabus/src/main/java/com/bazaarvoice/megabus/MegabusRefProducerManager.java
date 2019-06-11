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

    private static final int NUM_PARTITIONS = 8;
    private static final String LEADER_DIR = "/leader/partitioned-megabus-ref-producer";

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
                                     @MegabusApplicationId String applicationId,
                                     final RateLimitedLogFactory logFactory,
                                     @DatabusOstrichOwnerGroupFactory OstrichOwnerGroupFactory ownerGroupFactory,
                                     final MetricRegistry metricRegistry,
                                     KafkaCluster kafkaCluster,
                                     ObjectMapper objectMapper) {

        // Since the megabus reads from the databus's internal event store. We forward the refs directly onto a
        // kafka-based ref topic. We use a partitioned leader service to evenly balance the partitions among the megabus
        // servers.

        // TODO: since partitioned databus subscriptions are 1-based, we must add one to the partition condition. At some point in the future,
        // we should reconcile this inconsistency
        PartitionedServiceSupplier refProducerSupplier = partition ->
                new MegabusRefProducer(databusFactory.forOwner(systemId), databusEventStore,
                        Conditions.partition(NUM_PARTITIONS, partition + 1),
                        logFactory, metricRegistry, kafkaCluster.producer(), objectMapper, refTopic,
                        Integer.toString(partition), applicationId);

        PartitionedLeaderService partitionedLeaderService = new PartitionedLeaderService(
                curator, LEADER_DIR, hostAndPort.toString(),
                "PartitionedLeaderSelector-megabusRefProducer", NUM_PARTITIONS, 1, 1, TimeUnit.MINUTES,
                refProducerSupplier, clock);

        for (LeaderService leaderService : partitionedLeaderService.getPartitionLeaderServices()) {
            ServiceFailureListener.listenTo(leaderService, metricRegistry);
        }

        leaderServiceTask.register("megabus-ref-producer", partitionedLeaderService);
        lifeCycle.manage(partitionedLeaderService);
    }

}