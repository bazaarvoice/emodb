package com.bazaarvoice.megabus.refproducer;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.common.dropwizard.log.RateLimitedLogFactory;
import com.bazaarvoice.emodb.common.zookeeper.leader.PartitionedLeaderService;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.DatabusOstrichOwnerGroupFactory;
import com.bazaarvoice.emodb.databus.SystemIdentity;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.core.DatabusChannelConfiguration;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.bazaarvoice.emodb.databus.core.DatabusFactory;
import com.bazaarvoice.emodb.event.owner.OstrichOwnerGroupFactory;
import com.bazaarvoice.emodb.kafka.KafkaCluster;
import com.bazaarvoice.emodb.kafka.Topic;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.megabus.MegabusApplicationId;
import com.bazaarvoice.megabus.guice.MegabusRefTopic;
import com.bazaarvoice.megabus.guice.MegabusZookeeper;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import java.time.Clock;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;

import static java.util.Objects.requireNonNull;

public class MegabusRefProducerManager extends PartitionedLeaderService {

    private static final String LEADER_DIR = "/leader/partitioned-megabus-ref-producer";
    private static final String SERVICE_NAME = "megabus-ref-producer";

    private final Databus _databus;
    private final int _numRefPartitions;
    private final String _applicationId;

    @Inject
    public MegabusRefProducerManager(final LifeCycleRegistry lifeCycle,
                                     LeaderServiceTask leaderServiceTask,
                                     @MegabusZookeeper CuratorFramework curator,
                                     @MegabusRefTopic Topic refTopic,
                                     @SelfHostAndPort HostAndPort hostAndPort,
                                     Clock clock,
                                     final MegabusRefProducerConfiguration refProducerConfiguration,
                                     final DatabusFactory databusFactory,
                                     DatabusEventStore databusEventStore,
                                     final @SystemIdentity String systemId,
                                     @MegabusApplicationId String applicationId,
                                     final RateLimitedLogFactory logFactory,
                                     @DatabusOstrichOwnerGroupFactory OstrichOwnerGroupFactory ownerGroupFactory,
                                     @NumRefPartitions int numRefPartitions,
                                     final MetricRegistry metricRegistry,
                                     KafkaCluster kafkaCluster,
                                     ObjectMapper objectMapper) {

        // Since the megabus reads from the databus's internal event store. We forward the refs directly onto a
        // kafka-based ref topic. We use a partitioned leader service to evenly balance the partitions among the megabus
        // servers.
        super(curator, LEADER_DIR, hostAndPort.toString(),
                "PartitionedLeaderSelector-MegabusRefProducer", numRefPartitions, 1, 1, TimeUnit.MINUTES,
                partition -> new MegabusRefProducer(refProducerConfiguration, databusEventStore,
                                logFactory, metricRegistry, kafkaCluster.producer(), objectMapper, refTopic,
                                ChannelNames.getMegabusRefProducerChannel(applicationId, partition), Integer.toString(partition)), clock);

        _databus = requireNonNull(databusFactory).forOwner(systemId);
        _numRefPartitions = numRefPartitions;
        _applicationId = requireNonNull(applicationId);

        for (LeaderService leaderService : getPartitionLeaderServices()) {
            ServiceFailureListener.listenTo(leaderService, metricRegistry);
        }

        leaderServiceTask.register(SERVICE_NAME, this);
    }

    public void createRefSubscriptions() {
        // TODO: since partitioned databus subscriptions are 1-based, we must add one to the partition condition. At some point in the future,
        // we should reconcile this inconsistency

        // Except for resetting the ttl, recreating a subscription that already exists has no effect.
        // Assume that multiple servers that manage the same subscriptions can each attempt to create
        // the subscription at startup.  The subscription should last basically forever.
        for (int i = 0; i < _numRefPartitions; i++) {
            _databus.subscribe(ChannelNames.getMegabusRefProducerChannel(_applicationId, i),
                    Conditions.partition(_numRefPartitions, i + 1),
                    DatabusChannelConfiguration.MEGABUS_TTL, DatabusChannelConfiguration.MEGABUS_TTL, false);
        }
    }

}