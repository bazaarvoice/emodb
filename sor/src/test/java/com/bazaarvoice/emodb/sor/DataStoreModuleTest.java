package com.bazaarvoice.emodb.sor;

import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.common.cassandra.CqlDriverConfiguration;
import com.bazaarvoice.emodb.common.cassandra.KeyspaceConfiguration;
import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.datacenter.DataCenterConfiguration;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.core.SystemDataStore;
import com.bazaarvoice.emodb.sor.db.astyanax.AstyanaxDataReaderDAO;
import com.bazaarvoice.emodb.sor.db.astyanax.AstyanaxDataWriterDAO;
import com.bazaarvoice.emodb.sor.db.astyanax.CqlDataReaderDAO;
import com.bazaarvoice.emodb.sor.db.cql.CqlForMultiGets;
import com.bazaarvoice.emodb.sor.db.cql.CqlForScans;
import com.bazaarvoice.emodb.table.db.ClusterInfo;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxTableDAO;
import com.bazaarvoice.emodb.table.db.astyanax.SystemTablePlacement;
import com.bazaarvoice.emodb.table.db.consistency.GlobalFullConsistencyZooKeeper;
import com.bazaarvoice.emodb.table.db.generic.CachingTableDAO;
import com.bazaarvoice.emodb.table.db.generic.MutexTableDAO;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.sun.jersey.api.client.Client;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.utils.EnsurePath;
import org.joda.time.Period;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.time.Clock;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class DataStoreModuleTest {

    @Test
    public void testAllMode() {
        Injector injector = createInjector(EmoServiceMode.STANDARD_ALL);

        assertNotNull(injector.getInstance(DataStore.class));
        assertNotNull(injector.getInstance(DataProvider.class));
        assertNotNull(injector.getInstance(EventBus.class));

        assertTrue(injector.getInstance(DataProvider.class) == injector.getInstance(DataStore.class));

        // Verify that some things we expect to be private are, indeed, private
        assertPrivate(injector, MutexTableDAO.class);
        assertPrivate(injector, CachingTableDAO.class);
        assertPrivate(injector, AstyanaxTableDAO.class);
        assertPrivate(injector, AstyanaxDataReaderDAO.class);
        assertPrivate(injector, CqlDataReaderDAO.class);
        assertPrivate(injector, AstyanaxDataWriterDAO.class);
    }

    @Test
    public void testCliToolMode() {
        Injector injector = createInjector(EmoServiceMode.CLI_TOOL);

        assertNotNull(injector.getInstance(DataStore.class));
    }

    private Injector createInjector(final EmoServiceMode serviceMode) {
        // Mock the minimal CacheRegistry functionality required to instantiate the module
        final CacheRegistry rootCacheRegistry = mock(CacheRegistry.class);
        CacheRegistry sorCacheRegistry = mock(CacheRegistry.class);
        when(rootCacheRegistry.withNamespace(eq("sor"))).thenReturn(sorCacheRegistry);

        final CuratorFramework curator = mock(CuratorFramework.class);
        when(curator.getState()).thenReturn(CuratorFrameworkState.STARTED);
        when(curator.newNamespaceAwareEnsurePath(Mockito.<String>any())).thenReturn(mock(EnsurePath.class));

        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                binder().requireExplicitBindings();

                // construct the minimum necessary elements to allow a DataStore module to be created.
                bind(DataStoreConfiguration.class).toInstance(new DataStoreConfiguration()
                        .setHistoryTtl(Period.days(2))
                        .setValidTablePlacements(ImmutableSet.of("app_global:sys"))
                        .setCassandraClusters(ImmutableMap.of("app_global", new CassandraConfiguration()
                                .setCluster("Test Cluster")
                                .setSeeds("127.0.0.1")
                                .setPartitioner("bop")
                                .setKeyspaces(ImmutableMap.of(
                                        "app_global", new KeyspaceConfiguration())))));
                bind(String.class).annotatedWith(SystemTablePlacement.class).toInstance("app_global:sys");

                bind(DataStore.class).annotatedWith(SystemDataStore.class).toInstance(mock(DataStore.class));
                bind(DataCenterConfiguration.class).toInstance(new DataCenterConfiguration()
                        .setSystemDataCenter("datacenter1")
                        .setCurrentDataCenter("datacenter1"));

                bind(CqlDriverConfiguration.class).toInstance(new CqlDriverConfiguration());

                bind(HostAndPort.class).annotatedWith(SelfHostAndPort.class).toInstance(HostAndPort.fromString("localhost:8080"));
                bind(Client.class).toInstance(mock(Client.class));
                bind(CacheRegistry.class).toInstance(rootCacheRegistry);
                bind(DataCenters.class).toInstance(mock(DataCenters.class));
                bind(HealthCheckRegistry.class).toInstance(mock(HealthCheckRegistry.class));
                bind(LeaderServiceTask.class).toInstance(mock(LeaderServiceTask.class));
                bind(LifeCycleRegistry.class).toInstance(new SimpleLifeCycleRegistry());
                bind(TaskRegistry.class).toInstance(mock(TaskRegistry.class));
                bind(CuratorFramework.class).annotatedWith(Global.class).toInstance(curator);
                bind(CuratorFramework.class).annotatedWith(DataStoreZooKeeper.class).toInstance(curator);
                bind(CuratorFramework.class).annotatedWith(GlobalFullConsistencyZooKeeper.class).toInstance(curator);
                bind(ClusterInfo.class).toInstance(new ClusterInfo("Test Cluster", "Test Metric Cluster"));
                bind(MetricRegistry.class).asEagerSingleton();
                bind(JobService.class).toInstance(mock(JobService.class));
                bind(JobHandlerRegistry.class).toInstance(mock(JobHandlerRegistry.class));
                bind(new TypeLiteral<Supplier<Boolean>>(){}).annotatedWith(CqlForMultiGets.class).toInstance(Suppliers.ofInstance(true));
                bind(new TypeLiteral<Supplier<Boolean>>(){}).annotatedWith(CqlForScans.class).toInstance(Suppliers.ofInstance(true));
                bind(Clock.class).toInstance(Clock.systemDefaultZone());

                install(new DataStoreModule(serviceMode));
            }
        });

        verify(rootCacheRegistry).withNamespace("sor");
        //noinspection unchecked
        verify(sorCacheRegistry).register(eq("tables"), isA(Cache.class), eq(true));

        return injector;
    }

    private void assertPrivate(Injector injector, Class<?> type) {
        try {
            injector.getInstance(type);
            fail();
        } catch (ConfigurationException e) {
            // Expected
        }
    }
}
