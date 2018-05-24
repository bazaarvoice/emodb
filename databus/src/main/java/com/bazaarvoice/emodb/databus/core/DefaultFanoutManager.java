package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.common.zookeeper.leader.PartitionedLeaderService;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.DataCenterFanoutPartitions;
import com.bazaarvoice.emodb.databus.DatabusZooKeeper;
import com.bazaarvoice.emodb.databus.MasterFanoutPartitions;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.databus.repl.ReplicationEventSource;
import com.bazaarvoice.emodb.databus.repl.ReplicationSource;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.event.api.EventStore;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Multimap;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultFanoutManager implements FanoutManager {
    private static final Duration SAME_DC_SLEEP_WHEN_IDLE = Duration.ofMillis(100);
    private static final Duration REMOTE_DC_SLEEP_WHEN_IDLE = Duration.ofSeconds(1);

    private final EventStore _eventStore;
    private final SubscriptionDAO _subscriptionDao;
    private final DataCenters _dataCenters;
    private final CuratorFramework _curator;
    private final String _selfId;
    private final LeaderServiceTask _dropwizardTask;
    private final RateLimitedLogFactory _logFactory;
    private final SubscriptionEvaluator _subscriptionEvaluator;
    private final int _masterFanoutPartitions;
    private final int _dataCenterFanoutPartitions;
    private final PartitionSelector _dataCenterFanoutPartitionSelector;
    private final FanoutLagMonitor _fanoutLagMonitor;
    private final MetricRegistry _metricRegistry;
    private final Clock _clock;

    @Inject
    public DefaultFanoutManager(final EventStore eventStore, final SubscriptionDAO subscriptionDao,
                                SubscriptionEvaluator subscriptionEvaluator, DataCenters dataCenters,
                                @DatabusZooKeeper CuratorFramework curator, @SelfHostAndPort HostAndPort self,
                                @MasterFanoutPartitions int masterFanoutPartitions,
                                @DataCenterFanoutPartitions int dataCenterFanoutPartitions,
                                @DataCenterFanoutPartitions PartitionSelector dataCenterFanoutPartitionSelector,
                                FanoutLagMonitor fanoutLagMonitor,
                                LeaderServiceTask dropwizardTask, RateLimitedLogFactory logFactory,
                                MetricRegistry metricRegistry, Clock clock) {
        _eventStore = checkNotNull(eventStore, "eventStore");
        _subscriptionDao = checkNotNull(subscriptionDao, "subscriptionDao");
        _subscriptionEvaluator = checkNotNull(subscriptionEvaluator, "subscriptionEvaluator");
        _dataCenters = checkNotNull(dataCenters, "dataCenters");
        _curator = checkNotNull(curator, "curator");
        _selfId = checkNotNull(self, "self").toString();
        _dropwizardTask = checkNotNull(dropwizardTask, "dropwizardTask");
        _logFactory = checkNotNull(logFactory, "logFactory");
        _masterFanoutPartitions = masterFanoutPartitions;
        _dataCenterFanoutPartitions = dataCenterFanoutPartitions;
        _dataCenterFanoutPartitionSelector = checkNotNull(dataCenterFanoutPartitionSelector, "dataCenterFanoutPartitionSelector");
        _fanoutLagMonitor = checkNotNull(fanoutLagMonitor, "fanoutLagMonitor");
        _metricRegistry = metricRegistry;
        _clock = clock;
    }

    @Override
    public Managed newMasterFanout() {
        PartitionEventSourceSupplier eventSourceSupplier = partition ->
                new EventStoreEventSource(_eventStore, ChannelNames.getMasterFanoutChannel(partition));
        return create("master", eventSourceSupplier, _dataCenterFanoutPartitionSelector, SAME_DC_SLEEP_WHEN_IDLE, _masterFanoutPartitions);
    }

    @Override
    public Managed newInboundReplicationFanout(DataCenter dataCenter, ReplicationSource replicationSource) {
        PartitionEventSourceSupplier eventSourceSupplier = partition ->
                new ReplicationEventSource(replicationSource, ChannelNames.getReplicationFanoutChannel(_dataCenters.getSelf(), partition));
        return create("in-" + dataCenter.getName(), eventSourceSupplier, null, REMOTE_DC_SLEEP_WHEN_IDLE, _dataCenterFanoutPartitions);
    }

    private Managed create(final String name, final PartitionEventSourceSupplier eventSourceSupplier,
                           @Nullable final PartitionSelector outboundPartitionSelector, final Duration sleepWhenIdle,
                           final int partitions) {
        final Function<Multimap<String, ByteBuffer>, Void> eventSink = eventsByChannel -> {
            _eventStore.addAll(eventsByChannel);
            return null;
        };

        final Supplier<Iterable<OwnedSubscription>> subscriptionsSupplier = _subscriptionDao::getAllSubscriptions;

        PartitionedLeaderService partitionedLeaderService = new PartitionedLeaderService(
                _curator, ZKPaths.makePath("/leader/fanout", "partitioned-" + name),
                _selfId, "PartitionedLeaderSelector-" + name, partitions, 1,  1, TimeUnit.MINUTES,
                partition -> new DefaultFanout(name, "partition-" + partition,
                        eventSourceSupplier.createEventSourceForPartition(partition),
                        eventSink, outboundPartitionSelector, sleepWhenIdle, subscriptionsSupplier, _dataCenters.getSelf(),
                        _logFactory, _subscriptionEvaluator, _fanoutLagMonitor, _metricRegistry, _clock),
                _clock);

        for (LeaderService leaderService : partitionedLeaderService.getPartitionLeaderServices()) {
            ServiceFailureListener.listenTo(leaderService, _metricRegistry);
        }
        _dropwizardTask.register("databus-fanout-" + name, partitionedLeaderService);
        return partitionedLeaderService;
    }

    private interface PartitionEventSourceSupplier {
        EventSource createEventSourceForPartition(int partition);
    }
}