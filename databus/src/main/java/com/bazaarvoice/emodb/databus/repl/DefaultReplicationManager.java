package com.bazaarvoice.emodb.databus.repl;

import com.bazaarvoice.emodb.common.dropwizard.discovery.PayloadBuilder;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ManagedGuavaService;
import com.bazaarvoice.emodb.common.zookeeper.store.GuavaServiceController;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.databus.ReplicationEnabled;
import com.bazaarvoice.emodb.databus.ReplicationKey;
import com.bazaarvoice.emodb.databus.core.FanoutManager;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.bazaarvoice.ostrich.discovery.FixedHostDiscovery;
import com.bazaarvoice.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolProxies;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.sun.jersey.api.client.Client;
import io.dropwizard.lifecycle.ExecutorServiceManager;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Wakes up once a minute, checks which other data centers exist, and starts/stops inbound databus event replication
 * from those data centers.
 */
public class DefaultReplicationManager extends AbstractScheduledService {
    private static final Logger _log = LoggerFactory.getLogger(DefaultReplicationManager.class);

    private final ScheduledExecutorService _healthCheckExecutor;
    private final FanoutManager _fanoutManager;
    private final DataCenters _dataCenters;
    private final Client _jerseyClient;
    private final ValueStore<Boolean> _replicationEnabled;
    private final String _replicationApiKey;
    private final MetricRegistry _metrics;
    private final Map<String, Managed> _dataCenterFanout = Maps.newHashMap();

    @Inject
    public DefaultReplicationManager(LifeCycleRegistry lifeCycle, FanoutManager fanoutManager, DataCenters dataCenters,
                                     Client jerseyClient, @ReplicationEnabled ValueStore<Boolean> replicationEnabled,
                                     @ReplicationKey String replicationApiKey, MetricRegistry metrics) {
        _fanoutManager = requireNonNull(fanoutManager, "fanoutManager");
        _dataCenters = requireNonNull(dataCenters, "dataCenters");
        _jerseyClient = requireNonNull(jerseyClient, "jerseyClient");
        _replicationEnabled = requireNonNull(replicationEnabled, "replicationEnabled");
        _replicationApiKey = requireNonNull(replicationApiKey, "replicationApiKey");
        _metrics = requireNonNull(metrics, "metrics");
        _healthCheckExecutor = defaultScheduledExecutor(lifeCycle, "Databus Replication HealthCheck");

        lifeCycle.manage(new ManagedGuavaService(this));
    }

    private static ScheduledExecutorService defaultScheduledExecutor(LifeCycleRegistry lifeCycle, String nameFormat) {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
                new ThreadFactoryBuilder().setNameFormat(nameFormat).setDaemon(true).build());
        lifeCycle.manage(new ExecutorServiceManager(executor, Duration.seconds(5), nameFormat));
        return executor;
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedDelaySchedule(5, 60, TimeUnit.SECONDS);
    }

    @Override
    protected void shutDown() throws Exception {
        stopAll(_dataCenterFanout);
    }

    @Override
    protected void runOneIteration() throws Exception {
        try {
            // Start replication for all new data centers.
            Map<String, Managed> active = Maps.newHashMap(_dataCenterFanout);
            DataCenter self = _dataCenters.getSelf();
            for (DataCenter dataCenter : _dataCenters.getAll()) {
                if (dataCenter.equals(self)) {
                    continue;
                }
                Managed fanout = active.remove(dataCenter.getName());
                if (fanout == null) {
                    fanout = newInboundReplication(dataCenter);
                    try {
                        fanout.start();
                    } catch (Exception e) {
                        _log.error("Unexpected exception starting replication service: {}", dataCenter.getName());
                        continue;
                    }
                    _dataCenterFanout.put(dataCenter.getName(), fanout);
                }
            }

            // If a DataCenter has been removed, stop replicating from it.
            stopAll(active);

        } catch (Throwable t) {
            _log.error("Unexpected exception polling data center changes.", t);
        }
    }

    private void stopAll(Map<String, Managed> active) {
        // Copy the set to avoid concurrent modification exceptions
        for (Map.Entry<String, Managed> entry : Lists.newArrayList(active.entrySet())) {
            try {
                entry.getValue().stop();
            } catch (Exception e) {
                _log.error("Unexpected exception stopping replication service: {}", entry.getKey());
            }
            _dataCenterFanout.remove(entry.getKey());
        }
    }

    private Managed newInboundReplication(final DataCenter dataCenter) {
        // Create a proxy for the remote data center.
        final ReplicationSource replicationSource = newRemoteReplicationSource(dataCenter);

        // Start asynchronously downloading events from the remote data center.
        final Managed fanout = new GuavaServiceController(_replicationEnabled, () -> new AbstractService() {
            Managed _fanout = null;

            @Override
            protected void doStart() {
                _fanout = _fanoutManager.newInboundReplicationFanout(dataCenter, replicationSource);

                try {
                    _fanout.start();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
                notifyStarted();
            }

            @Override
            protected void doStop() {
                try {
                    if (_fanout != null) {
                        _fanout.stop();
                        _fanout = null;
                    }
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
                notifyStopped();
            }
        });

        // Note: closing the replication source could also be done via a listener on the Guava service...
        return new Managed() {
            @Override
            public void start() throws Exception {
                fanout.start();
            }

            @Override
            public void stop() throws Exception {
                fanout.stop();
                ServicePoolProxies.close(replicationSource);
            }
        };
    }

    /** Creates a ReplicationSource proxy to the remote data center. */
    private ReplicationSource newRemoteReplicationSource(DataCenter dataCenter) {
        MultiThreadedServiceFactory<ReplicationSource> clientFactory = new ReplicationClientFactory(_jerseyClient)
                .usingApiKey(_replicationApiKey);

        ServiceEndPoint endPoint = new ServiceEndPointBuilder()
                .withServiceName(clientFactory.getServiceName())
                .withId(dataCenter.getName())
                .withPayload(new PayloadBuilder()
                        .withUrl(dataCenter.getServiceUri().resolve(ReplicationClient.SERVICE_PATH))
                        .withAdminUrl(dataCenter.getAdminUri())
                        .toString())
                .build();

        return ServicePoolBuilder.create(ReplicationSource.class)
                .withHostDiscovery(new FixedHostDiscovery(endPoint))
                .withServiceFactory(clientFactory)
                .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                .withHealthCheckExecutor(_healthCheckExecutor)
                .withMetricRegistry(_metrics)
                .buildProxy(new ExponentialBackoffRetry(30, 1, 10, TimeUnit.SECONDS));
    }
}
