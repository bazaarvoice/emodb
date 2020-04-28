package com.bazaarvoice.emodb.blob;

import com.amazonaws.regions.Regions;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.core.SystemBlobStore;
import com.bazaarvoice.emodb.blob.db.astyanax.AstyanaxStorageProvider;
import com.bazaarvoice.emodb.blob.db.s3.config.S3BucketConfiguration;
import com.bazaarvoice.emodb.blob.db.s3.config.S3Configuration;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.common.cassandra.KeyspaceConfiguration;
import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPort;
import com.bazaarvoice.emodb.common.dropwizard.guice.SystemTablePlacement;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.leader.LeaderServiceTask;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.datacenter.DataCenterConfiguration;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.table.db.TableBackingStore;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxTableDAO;
import com.bazaarvoice.emodb.table.db.consistency.GlobalFullConsistencyZooKeeper;
import com.bazaarvoice.emodb.table.db.generic.CachingTableDAO;
import com.bazaarvoice.emodb.table.db.generic.MutexTableDAO;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.Cache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.inject.AbstractModule;
import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.sun.jersey.api.client.Client;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.utils.EnsurePath;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.time.Clock;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class BlobStoreModuleTest {

    @Test
    public void testWebServer() {
        Injector injector = createInjector(EmoServiceMode.STANDARD_ALL);

        assertNotNull(injector.getInstance(BlobStore.class));

        // Verify that some things we expect to be private are, indeed, private
        assertPrivate(injector, MutexTableDAO.class);
        assertPrivate(injector, CachingTableDAO.class);
        assertPrivate(injector, AstyanaxTableDAO.class);
        assertPrivate(injector, AstyanaxStorageProvider.class);
    }

    @Test
    public void testMainRoleMode() {
        Injector injector = createInjector(EmoServiceMode.STANDARD_MAIN);

        assertNotNull(injector.getInstance(BlobStore.class));
    }

    @Test
    public void testBlobRoleMode() {
        Injector injector = createInjector(EmoServiceMode.STANDARD_BLOB);

        assertNotNull(injector.getInstance(BlobStore.class));
    }

    @Test
    public void testCliTool() {
        Injector injector = createInjector(EmoServiceMode.CLI_TOOL);

        assertNotNull(injector.getInstance(BlobStore.class));
    }

    private Injector createInjector(final EmoServiceMode serviceMode) {
        // Mock the minimal CacheRegistry functionality required to instantiate the module
        final CacheRegistry rootCacheRegistry = mock(CacheRegistry.class);
        CacheRegistry blobCacheRegistry = mock(CacheRegistry.class);
        when(rootCacheRegistry.withNamespace(eq("blob"))).thenReturn(blobCacheRegistry);

        final CuratorFramework curator = mock(CuratorFramework.class);
        when(curator.getState()).thenReturn(CuratorFrameworkState.STARTED);
        when(curator.newNamespaceAwareEnsurePath(Mockito.<String>any())).thenReturn(mock(EnsurePath.class));

        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                binder().requireExplicitBindings();

                // construct the minimum necessary elements to allow a BlobStore module to be created.
                bind(BlobStoreConfiguration.class).toInstance(new BlobStoreConfiguration()
                        .setValidTablePlacements(ImmutableSet.of("media_global:ugc"))
                        .setCassandraClusters(ImmutableMap.of("media_global", new CassandraConfiguration()
                                .setCluster("Test Cluster")
                                .setSeeds("127.0.0.1")
                                .setPartitioner("bop")
                                .setKeyspaces(ImmutableMap.of(
                                        "media_global", new KeyspaceConfiguration())))
                        )
                        .setS3Configuration(new S3Configuration()
                                .setS3BucketConfigurations(ImmutableList.of(new S3BucketConfiguration("local-emodb--media-global-ugc", Regions.DEFAULT_REGION.getName(), null, null, false, 1, 1, 1, null))))
                );

                bind(String.class).annotatedWith(SystemTablePlacement.class).toInstance("ugc_global:sys");

                bind(DataCenterConfiguration.class).toInstance(new DataCenterConfiguration()
                        .setSystemDataCenter("datacenter1")
                        .setCurrentDataCenter("datacenter1"));

                bind(BlobStore.class).annotatedWith(SystemBlobStore.class).toInstance(mock(BlobStore.class));
                bind(HostAndPort.class).annotatedWith(SelfHostAndPort.class).toInstance(HostAndPort.fromString("localhost:8080"));
                bind(Client.class).toInstance(mock(Client.class));
                bind(CacheRegistry.class).toInstance(rootCacheRegistry);
                bind(DataCenters.class).toInstance(mock(DataCenters.class));
                bind(TableBackingStore.class).toInstance(mock(TableBackingStore.class));
                bind(HealthCheckRegistry.class).toInstance(mock(HealthCheckRegistry.class));
                bind(LeaderServiceTask.class).toInstance(mock(LeaderServiceTask.class));
                bind(LifeCycleRegistry.class).toInstance(new SimpleLifeCycleRegistry());
                bind(TaskRegistry.class).toInstance(mock(TaskRegistry.class));
                bind(CuratorFramework.class).annotatedWith(Global.class).toInstance(curator);
                bind(CuratorFramework.class).annotatedWith(GlobalFullConsistencyZooKeeper.class).toInstance(curator);
                bind(CuratorFramework.class).annotatedWith(BlobStoreZooKeeper.class).toInstance(curator);

                MetricRegistry metricRegistry = new MetricRegistry();
                bind(MetricRegistry.class).toInstance(metricRegistry);

                bind(Clock.class).toInstance(Clock.systemDefaultZone());

                install(new BlobStoreModule(serviceMode, "bv.emodb.blob", metricRegistry));
            }
        });

        verify(rootCacheRegistry).withNamespace("blob");
        //noinspection unchecked
        verify(blobCacheRegistry).register(eq("tables"), isA(Cache.class), eq(true));

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
