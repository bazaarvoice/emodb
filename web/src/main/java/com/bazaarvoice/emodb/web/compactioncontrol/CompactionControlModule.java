package com.bazaarvoice.emodb.web.compactioncontrol;

import com.bazaarvoice.emodb.common.dropwizard.discovery.PayloadBuilder;
import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.bazaarvoice.emodb.common.zookeeper.store.ZkMapStore;
import com.bazaarvoice.emodb.datacenter.DataCenterConfiguration;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.bazaarvoice.emodb.sor.client.CompactionControlClientFactory;
import com.bazaarvoice.emodb.sor.client.DataStoreClient;
import com.bazaarvoice.emodb.sor.compactioncontrol.CompControlApiKey;
import com.bazaarvoice.emodb.sor.compactioncontrol.DefaultCompactionControlSource;
import com.bazaarvoice.emodb.sor.compactioncontrol.DelegateCompactionControl;
import com.bazaarvoice.emodb.sor.compactioncontrol.LocalCompactionControl;
import com.bazaarvoice.emodb.sor.compactioncontrol.StashRunTimeInfoSerializer;
import com.bazaarvoice.emodb.sor.compactioncontrol.StashRunTimeMapStore;
import com.bazaarvoice.emodb.table.db.astyanax.CurrentDataCenter;
import com.bazaarvoice.emodb.table.db.consistency.GlobalFullConsistencyZooKeeper;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.ServiceEndPointBuilder;
import com.bazaarvoice.ostrich.discovery.FixedHostDiscovery;
import com.bazaarvoice.ostrich.pool.ServiceCachingPolicyBuilder;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.bazaarvoice.ostrich.retry.ExponentialBackoffRetry;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;

import javax.ws.rs.client.Client;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Guice module
 * <p/>
 * Requires the following external references:
 * <ul>
 * <li> {@link DataCenters}
 * <li> {@link MetricRegistry}
 * <li> Jersey {@link Client}
 * </ul>
 * Exports the following:
 * <ul>
 * <li> {@link CompactionControlSource}
 * </ul>
 */
public class CompactionControlModule extends PrivateModule {

    @Override
    protected void configure() {
        bind(CompactionControlSource.class).annotatedWith(LocalCompactionControl.class).to(DefaultCompactionControlSource.class).asEagerSingleton();
        expose(CompactionControlSource.class).annotatedWith(LocalCompactionControl.class);

        bind(CompactionControlSource.class).annotatedWith(DelegateCompactionControl.class).to(DelegateCompactionControlSource.class).asEagerSingleton();
        expose(CompactionControlSource.class).annotatedWith(DelegateCompactionControl.class);
    }

    @Provides
    @Singleton
    @CurrentDataCenter
    String provideDataCenters(DataCenterConfiguration dataCenterConfiguration) {
        return dataCenterConfiguration.getCassandraDataCenter();
    }

    @Provides
    @Singleton
    @StashRunTimeMapStore
    MapStore<StashRunTimeInfo> provideStashRunTimeValues(@CurrentDataCenter String currentDataCenter, @GlobalFullConsistencyZooKeeper CuratorFramework curator,
                                                         LifeCycleRegistry lifeCycle)
            throws Exception {
        // appending the current datacenter name in the zookeeper path. So this means the values stored here are datacenter specific.
        String zkPath = ZKPaths.makePath("/stash-running-instance/start-timestamp", currentDataCenter);
        return lifeCycle.manage(new ZkMapStore<>(curator, zkPath, new StashRunTimeInfoSerializer()));
    }

    @Provides
    @Singleton
    @AllCompactionControlSources
    public List<CompactionControlSource> getAllCompactionControlSources(@LocalCompactionControl CompactionControlSource localCompactionControlSource, @ServerCluster String serverCluster,
                                                                        Client client, DataCenters dataCenters, @CompControlApiKey String compControlApiKey,
                                                                        HealthCheckRegistry healthCheckRegistry, MetricRegistry metrics) {
        List<CompactionControlSource> compactionControlSources = Lists.newArrayList();
        for (DataCenter dataCenter : dataCenters.getAll()) {
            MultiThreadedServiceFactory<CompactionControlSource> clientFactory = new CompactionControlClientFactory(serverCluster, new JerseyEmoClient(client), compControlApiKey);

            if (dataCenter.equals(dataCenters.getSelf())) {
                compactionControlSources.add(localCompactionControlSource);
            } else {
                ServiceEndPoint endPoint = new ServiceEndPointBuilder()
                        .withServiceName(clientFactory.getServiceName())
                        .withId(dataCenter.getName())
                        .withPayload(new PayloadBuilder()
                                .withUrl(dataCenter.getServiceUri().resolve(DataStoreClient.SERVICE_PATH))
                                .withAdminUrl(dataCenter.getAdminUri())
                                .toString())
                        .build();

                compactionControlSources.add(ServicePoolBuilder.create(CompactionControlSource.class)
                        .withHostDiscovery(new FixedHostDiscovery(endPoint))
                        .withServiceFactory(clientFactory)
                        .withCachingPolicy(ServiceCachingPolicyBuilder.getMultiThreadedClientPolicy())
                        .withMetricRegistry(metrics)
                        .buildProxy(new ExponentialBackoffRetry(30, 1, 10, TimeUnit.SECONDS)));
            }
        }
        return compactionControlSources;
    }

}