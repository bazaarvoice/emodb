package test.integration.databus;

import com.bazaarvoice.emodb.cachemgr.CacheManagerModule;
import com.bazaarvoice.emodb.cachemgr.invalidate.InvalidationService;
import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.common.cassandra.health.CassandraHealthCheck;
import com.bazaarvoice.emodb.common.cassandra.test.TestCassandraConfiguration;
import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPortModule;
import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.databus.DefaultJoinFilter;
import com.bazaarvoice.emodb.databus.DatabusConfiguration;
import com.bazaarvoice.emodb.databus.DatabusHostDiscovery;
import com.bazaarvoice.emodb.databus.DatabusModule;
import com.bazaarvoice.emodb.databus.DatabusZooKeeper;
import com.bazaarvoice.emodb.databus.ReplicationKey;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.datacenter.DataCenterConfiguration;
import com.bazaarvoice.emodb.datacenter.DataCenterModule;
import com.bazaarvoice.emodb.datacenter.api.KeyspaceDiscovery;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.sor.DataStoreConfiguration;
import com.bazaarvoice.emodb.sor.DataStoreModule;
import com.bazaarvoice.emodb.sor.DataStoreZooKeeper;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.SystemDataStore;
import com.bazaarvoice.emodb.table.db.consistency.GlobalFullConsistencyZooKeeper;
import com.bazaarvoice.emodb.web.util.ZKNamespaces;
import com.bazaarvoice.ostrich.HostDiscovery;
import com.bazaarvoice.ostrich.ServiceRegistry;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.server.SimpleServerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.joda.time.Period;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class CasDatabusTest {
    private SimpleLifeCycleRegistry _lifeCycle;
    private HealthCheckRegistry _healthChecks;
    private Databus _bus;

    @BeforeClass
    public void setup() throws Exception {
        _lifeCycle = new SimpleLifeCycleRegistry();
        _healthChecks = mock(HealthCheckRegistry.class);

        // Start test instance of ZooKeeper in the current JVM
        TestingServer testingServer = new TestingServer();
        _lifeCycle.manage(testingServer);

        // Connect to ZooKeeper
        final CuratorFramework curator = CuratorFrameworkFactory.newClient(testingServer.getConnectString(),
                new BoundedExponentialBackoffRetry(100, 1000, 5));
        _lifeCycle.manage(curator).start();

        final MetricRegistry metricRegistry = new MetricRegistry();

        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(LifeCycleRegistry.class).toInstance(_lifeCycle);
                bind(HealthCheckRegistry.class).toInstance(_healthChecks);
                bind(TaskRegistry.class).toInstance(mock(TaskRegistry.class));

                CassandraConfiguration cassandraConfiguration = new TestCassandraConfiguration("databus", "subscription");
                bind(DatabusConfiguration.class).toInstance(new DatabusConfiguration()
                        .setCassandraConfiguration(cassandraConfiguration));

                bind(DataStoreConfiguration.class).toInstance(new DataStoreConfiguration()
                        .setSystemTablePlacement("app_global:sys")
                        .setValidTablePlacements(ImmutableSet.of("app_global:sys", "ugc_global:ugc", "app_remote:default"))
                        .setCassandraClusters(ImmutableMap.<String, CassandraConfiguration>of(
                                "ugc_global", new TestCassandraConfiguration("ugc_global", "ugc_delta"),
                                "app_global", new TestCassandraConfiguration("app_global", "sys_delta")))
                        .setHistoryTtl(Period.days(2)));
                bind(DataStore.class).annotatedWith(SystemDataStore.class).toInstance(mock(DataStore.class));

                bind(DataCenterConfiguration.class).toInstance(new DataCenterConfiguration()
                        .setCurrentDataCenter("datacenter1")
                        .setSystemDataCenter("datacenter1")
                        .setDataCenterServiceUri(URI.create("http://localhost:8080"))
                        .setDataCenterAdminUri(URI.create("http://localhost:8080")));
                bind(KeyspaceDiscovery.class).annotatedWith(Names.named("blob")).toInstance(mock(KeyspaceDiscovery.class));
                bind(String.class).annotatedWith(ServerCluster.class).toInstance("local_default");

                bind(String.class).annotatedWith(ReplicationKey.class).toInstance("password");
                bind(String.class).annotatedWith(InvalidationService.class).toInstance("emodb-cachemgr");

                bind(CuratorFramework.class).annotatedWith(Global.class).toInstance(curator);
                bind(CuratorFramework.class).annotatedWith(DatabusZooKeeper.class)
                        .toInstance(ZKNamespaces.usingChildNamespace(curator, "applications/emodb-bus"));
                bind(CuratorFramework.class).annotatedWith(DataStoreZooKeeper.class)
                        .toInstance(ZKNamespaces.usingChildNamespace(curator, "applications/emodb-sor"));
                bind(CuratorFramework.class).annotatedWith(GlobalFullConsistencyZooKeeper.class)
                        .toInstance(ZKNamespaces.usingChildNamespace(curator, "applications/emodb-fct"));

                bind(HostDiscovery.class).annotatedWith(DatabusHostDiscovery.class)
                        .toInstance(new ZooKeeperHostDiscovery(
                                ZKNamespaces.usingChildNamespace(curator, "applications/emodb-bus"),
                                "local_default-emodb-bus-1", metricRegistry));

                bind(ServerFactory.class).toInstance(new SimpleServerFactory());

                bind(ServiceRegistry.class).toInstance(mock(ServiceRegistry.class));

                bind(JobService.class).toInstance(mock(JobService.class));
                bind(JobHandlerRegistry.class).toInstance(mock(JobHandlerRegistry.class));

                bind(new TypeLiteral<Supplier<Condition>>(){}).annotatedWith(DefaultJoinFilter.class)
                        .toInstance(Suppliers.ofInstance(Conditions.alwaysFalse()));

                EmoServiceMode serviceMode = EmoServiceMode.STANDARD_ALL;
                install(new SelfHostAndPortModule());
                install(new DataCenterModule(serviceMode));
                install(new CacheManagerModule());
                install(new DataStoreModule(serviceMode));
                install(new DatabusModule(serviceMode, metricRegistry));
            }
        });
        _bus = injector.getInstance(Databus.class);

        _lifeCycle.start();
    }

    @AfterClass
    public void teardown() throws Exception {
        _lifeCycle.stop();
    }

    @Test
    public void testHealthCheck() throws Exception {
        ArgumentCaptor<HealthCheck> captor = ArgumentCaptor.forClass(HealthCheck.class);
        verify(_healthChecks, atLeastOnce()).addHealthCheck(Matchers.anyString(), captor.capture());
        List<HealthCheck> healthChecks = captor.getAllValues();

        int numCassandraHealthChecks = 0;
        for (HealthCheck healthCheck : healthChecks) {
            if (healthCheck instanceof CassandraHealthCheck) {
                HealthCheck.Result result = healthCheck.execute();
                assertTrue(result.isHealthy(), result.getMessage());
                numCassandraHealthChecks++;
            }
        }
        assertEquals(numCassandraHealthChecks, 3);  // app, ugc, databus
    }
}
