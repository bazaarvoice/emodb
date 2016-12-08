package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.curator.recipes.leader.LeaderService;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.DatabusZooKeeper;
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
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultFanoutManager implements FanoutManager {
    private static final Duration SAME_DC_SLEEP_WHEN_IDLE = Duration.millis(100);
    private static final Duration REMOTE_DC_SLEEP_WHEN_IDLE = Duration.standardSeconds(1);

    private final EventStore _eventStore;
    private final SubscriptionDAO _subscriptionDao;
    private final DataCenters _dataCenters;
    private final CuratorFramework _curator;
    private final String _selfId;
    private final LeaderServiceTask _dropwizardTask;
    private final RateLimitedLogFactory _logFactory;
    private final SubscriptionEvaluator _subscriptionEvaluator;
    private final MetricRegistry _metricRegistry;

    @Inject
    public DefaultFanoutManager(final EventStore eventStore, final SubscriptionDAO subscriptionDao,
                                SubscriptionEvaluator subscriptionEvaluator, DataCenters dataCenters,
                                @DatabusZooKeeper CuratorFramework curator, @SelfHostAndPort HostAndPort self,
                                LeaderServiceTask dropwizardTask, RateLimitedLogFactory logFactory, MetricRegistry metricRegistry) {
        _eventStore = checkNotNull(eventStore, "eventStore");
        _subscriptionDao = checkNotNull(subscriptionDao, "subscriptionDao");
        _subscriptionEvaluator = checkNotNull(subscriptionEvaluator, "subscriptionEvaluator");
        _dataCenters = checkNotNull(dataCenters, "dataCenters");
        _curator = checkNotNull(curator, "curator");
        _selfId = checkNotNull(self, "self").toString();
        _dropwizardTask = checkNotNull(dropwizardTask, "dropwizardTask");
        _logFactory = checkNotNull(logFactory, "logFactory");
        _metricRegistry = metricRegistry;
    }

    @Override
    public Service newMasterFanout() {
        String sourceChannel = ChannelNames.getMasterFanoutChannel();
        EventStoreEventSource eventSource = new EventStoreEventSource(_eventStore, sourceChannel);
        return create("master", eventSource, true, SAME_DC_SLEEP_WHEN_IDLE);
    }

    @Override
    public Service newInboundReplicationFanout(DataCenter dataCenter, ReplicationSource replicationSource) {
        String sourceChannel = ChannelNames.getReplicationFanoutChannel(_dataCenters.getSelf());
        EventSource eventSource = new ReplicationEventSource(replicationSource, sourceChannel);
        return create("in-" + dataCenter.getName(), eventSource, false, REMOTE_DC_SLEEP_WHEN_IDLE);
    }

    private Service create(final String name, final EventSource eventSource, final boolean replicateOutbound, final Duration sleepWhenIdle) {
        final Function<Multimap<String, ByteBuffer>, Void> eventSink = new Function<Multimap<String, ByteBuffer>, Void>() {
            @Override
            public Void apply(@Nullable Multimap<String, ByteBuffer> eventsByChannel) {
                _eventStore.addAll(eventsByChannel);
                return null;
            }
        };
        final Supplier<Iterable<OwnedSubscription>> subscriptionsSupplier = () -> _subscriptionDao.getAllSubscriptions();

        LeaderService leaderService = new LeaderService(
                _curator, ZKPaths.makePath("/leader/fanout", name), _selfId, "LeaderSelector-" + name, 1, TimeUnit.MINUTES,
                new Supplier<Service>() {
                    @Override
                    public Service get() {
                        return new DefaultFanout(name, eventSource, eventSink, replicateOutbound, sleepWhenIdle,
                                subscriptionsSupplier, _dataCenters.getSelf(), _logFactory, _subscriptionEvaluator,
                                _metricRegistry);
                    }
                });
        ServiceFailureListener.listenTo(leaderService, _metricRegistry);
        _dropwizardTask.register("databus-fanout", leaderService);
        return leaderService;
    }
}