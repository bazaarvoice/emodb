package com.bazaarvoice.emodb.databus;

import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.common.cassandra.KeyspaceConfiguration;
import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.table.db.ClusterInfo;
import com.bazaarvoice.emodb.table.db.Placements;
import com.bazaarvoice.emodb.table.db.consistency.DatabusClusterInfo;
import com.bazaarvoice.ostrich.HostDiscovery;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.EventBus;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.sun.jersey.api.client.Client;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.utils.EnsurePath;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.time.Clock;
import java.util.Collection;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;

public class DatabusModuleTest {

    @Test
    public void testWebServer() {
        Injector injector = createInjector(EmoServiceMode.STANDARD_ALL);

        assertNotNull(injector.getInstance(Databus.class));
    }

    @Test
    public void testCliTool() {
        Injector injector = createInjector(EmoServiceMode.CLI_TOOL);

        assertNotNull(injector.getInstance(Databus.class));
    }

    private Injector createInjector(final EmoServiceMode serviceMode) {
        // Mock the minimal CacheRegistry functionality required to instantiate the module
        final CacheRegistry rootCacheRegistry = mock(CacheRegistry.class);
        CacheRegistry sorCacheRegistry = mock(CacheRegistry.class);
        when(rootCacheRegistry.withNamespace(eq("bus"))).thenReturn(sorCacheRegistry);

        final CuratorFramework curator = mock(CuratorFramework.class);
        when(curator.getState()).thenReturn(CuratorFrameworkState.STARTED);
        when(curator.newNamespaceAwareEnsurePath(Mockito.<String>any())).thenReturn(mock(EnsurePath.class));

        return Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                binder().requireExplicitBindings();

                // construct the minimum necessary elements to allow a Databus module to be created.
                bind(DatabusConfiguration.class).toInstance(new DatabusConfiguration()
                        .setCassandraConfiguration(new CassandraConfiguration()
                                .setCluster("Test Cluster")
                                .setSeeds("127.0.0.1")
                                .setPartitioner("random")
                                .setKeyspaces(ImmutableMap.of(
                                        "random", new KeyspaceConfiguration().setHealthCheckColumnFamily("subscription")))));

                bind(CacheRegistry.class).toInstance(rootCacheRegistry);
                bind(DataCenters.class).toInstance(mock(DataCenters.class));
                bind(HealthCheckRegistry.class).toInstance(mock(HealthCheckRegistry.class));
                bind(LeaderServiceTask.class).toInstance(mock(LeaderServiceTask.class));
                bind(LifeCycleRegistry.class).toInstance(new SimpleLifeCycleRegistry());
                bind(TaskRegistry.class).toInstance(mock(TaskRegistry.class));
                bind(HostAndPort.class).annotatedWith(SelfHostAndPort.class).toInstance(HostAndPort.fromString("localhost:8080"));
                bind(Client.class).toInstance(mock(Client.class));
                bind(DataProvider.class).toInstance(mock(DataProvider.class));
                bind(Placements.class).toInstance(mock(Placements.class));
                bind(EventBus.class).asEagerSingleton();
                bind(CuratorFramework.class).annotatedWith(Global.class).toInstance(curator);
                bind(CuratorFramework.class).annotatedWith(DatabusZooKeeper.class).toInstance(curator);
                bind(HostDiscovery.class).annotatedWith(DatabusHostDiscovery.class).toInstance(mock(HostDiscovery.class));
                bind(String.class).annotatedWith(ReplicationKey.class).toInstance("password");
                bind(new TypeLiteral<Collection<ClusterInfo>>(){}).annotatedWith(DatabusClusterInfo.class)
                        .toInstance(ImmutableList.of(new ClusterInfo("Test Cluster", "Test Metric Cluster")));
                bind(JobService.class).toInstance(mock(JobService.class));
                bind(JobHandlerRegistry.class).toInstance(mock(JobHandlerRegistry.class));
                bind(new TypeLiteral<Supplier<Condition>>(){}).annotatedWith(DefaultJoinFilter.class)
                        .toInstance(Suppliers.ofInstance(Conditions.alwaysFalse()));

                MetricRegistry metricRegistry = new MetricRegistry();
                bind(MetricRegistry.class).toInstance(metricRegistry);
                bind(Clock.class).toInstance(mock(Clock.class));

                install(new DatabusModule(serviceMode, metricRegistry));
            }
        });
    }
}
