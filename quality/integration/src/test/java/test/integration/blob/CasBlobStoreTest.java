package test.integration.blob;

import com.bazaarvoice.emodb.blob.BlobStoreConfiguration;
import com.bazaarvoice.emodb.blob.BlobStoreModule;
import com.bazaarvoice.emodb.blob.BlobStoreZooKeeper;
import com.bazaarvoice.emodb.blob.api.Blob;
import com.bazaarvoice.emodb.blob.api.BlobMetadata;
import com.bazaarvoice.emodb.blob.api.BlobNotFoundException;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.Table;
import com.bazaarvoice.emodb.blob.core.SystemBlobStore;
import com.bazaarvoice.emodb.cachemgr.CacheManagerModule;
import com.bazaarvoice.emodb.cachemgr.invalidate.InvalidationService;
import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.common.cassandra.CqlDriverConfiguration;
import com.bazaarvoice.emodb.common.cassandra.health.CassandraHealthCheck;
import com.bazaarvoice.emodb.common.cassandra.test.TestCassandraConfiguration;
import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.guice.SelfHostAndPortModule;
import com.bazaarvoice.emodb.common.dropwizard.guice.ServerCluster;
import com.bazaarvoice.emodb.common.dropwizard.guice.SystemTablePlacement;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.service.EmoServiceMode;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.datacenter.DataCenterConfiguration;
import com.bazaarvoice.emodb.datacenter.DataCenterModule;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.sor.DataStoreConfiguration;
import com.bazaarvoice.emodb.sor.DataStoreModule;
import com.bazaarvoice.emodb.sor.DataStoreZooKeeper;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.compactioncontrol.CompControlApiKey;
import com.bazaarvoice.emodb.sor.compactioncontrol.LocalCompactionControl;
import com.bazaarvoice.emodb.sor.core.SystemDataStore;
import com.bazaarvoice.emodb.sor.db.cql.CqlForMultiGets;
import com.bazaarvoice.emodb.sor.db.cql.CqlForScans;
import com.bazaarvoice.emodb.table.db.consistency.GlobalFullConsistencyZooKeeper;
import com.bazaarvoice.emodb.web.util.ZKNamespaces;
import com.bazaarvoice.ostrich.ServiceRegistry;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.InputSupplier;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.server.SimpleServerFactory;
import io.dropwizard.setup.Environment;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import javax.validation.Validation;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class CasBlobStoreTest {
    private static final String TABLE = "test" + UUID.randomUUID().toString();

    private final Random _random = new Random();
    private SimpleLifeCycleRegistry _lifeCycle;
    private HealthCheckRegistry _healthChecks;
    private BlobStore _store;

    @BeforeClass
    public void setup() throws Exception {
        _lifeCycle = new SimpleLifeCycleRegistry();
        _healthChecks = mock(HealthCheckRegistry.class);

        // Start test instance of ZooKeeper in the current JVM
        TestingServer testingServer = new TestingServer();
        _lifeCycle.manage(testingServer);

        // Connect to ZooKeeper
        RetryPolicy retry = new BoundedExponentialBackoffRetry(100, 1000, 5);
        final CuratorFramework curator = CuratorFrameworkFactory.newClient(testingServer.getConnectString(), retry);
        _lifeCycle.manage(curator).start();

        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(LifeCycleRegistry.class).toInstance(_lifeCycle);
                bind(HealthCheckRegistry.class).toInstance(_healthChecks);
                bind(TaskRegistry.class).toInstance(mock(TaskRegistry.class));

                bind(BlobStoreConfiguration.class).toInstance(new BlobStoreConfiguration()
                        .setValidTablePlacements(ImmutableSet.of("media_global:ugc"))
                        .setCassandraClusters(ImmutableMap.<String, CassandraConfiguration>of(
                                "media_global", new TestCassandraConfiguration("media_global", "ugc_blob"))));

                bind(DataStoreConfiguration.class).toInstance(new DataStoreConfiguration()
                        .setValidTablePlacements(ImmutableSet.of("app_global:sys", "ugc_global:ugc"))
                        .setCassandraClusters(ImmutableMap.<String, CassandraConfiguration>of(
                                "ugc_global", new TestCassandraConfiguration("ugc_global", "ugc_delta"),
                                "app_global", new TestCassandraConfiguration("app_global", "sys_delta")))
                        .setHistoryTtl(Duration.ofDays(2)));

                bind(String.class).annotatedWith(SystemTablePlacement.class).toInstance("app_global:sys");

                bind(DataStore.class).annotatedWith(SystemDataStore.class).toInstance(mock(DataStore.class));
                bind(BlobStore.class).annotatedWith(SystemBlobStore.class).toInstance(mock(BlobStore.class));
                bind(JobService.class).toInstance(mock(JobService.class));
                bind(JobHandlerRegistry.class).toInstance(mock(JobHandlerRegistry.class));

                bind(DataCenterConfiguration.class).toInstance(new DataCenterConfiguration()
                        .setCurrentDataCenter("datacenter1")
                        .setSystemDataCenter("datacenter1")
                        .setDataCenterServiceUri(URI.create("http://localhost:8080"))
                        .setDataCenterAdminUri(URI.create("http://localhost:8080")));

                bind(CqlDriverConfiguration.class).toInstance(new CqlDriverConfiguration());

                bind(String.class).annotatedWith(ServerCluster.class).toInstance("local_default");
                bind(String.class).annotatedWith(InvalidationService.class).toInstance("emodb-cachemgr");

                bind(CuratorFramework.class).annotatedWith(Global.class).toInstance(curator);
                bind(CuratorFramework.class).annotatedWith(BlobStoreZooKeeper.class)
                        .toInstance(ZKNamespaces.usingChildNamespace(curator, "applications/emodb-blob"));
                bind(CuratorFramework.class).annotatedWith(DataStoreZooKeeper.class)
                        .toInstance(ZKNamespaces.usingChildNamespace(curator, "applications/emodb-sor"));
                bind(CuratorFramework.class).annotatedWith(GlobalFullConsistencyZooKeeper.class)
                        .toInstance(ZKNamespaces.usingChildNamespace(curator, "applications/emodb-fct"));

                bind(new TypeLiteral<Supplier<Boolean>>(){}).annotatedWith(CqlForScans.class)
                        .toInstance(Suppliers.ofInstance(true));
                bind(new TypeLiteral<Supplier<Boolean>>(){}).annotatedWith(CqlForMultiGets.class)
                        .toInstance(Suppliers.ofInstance(true));

                bind(ServerFactory.class).toInstance(new SimpleServerFactory());

                bind(ServiceRegistry.class).toInstance(mock(ServiceRegistry.class));

                bind(Clock.class).toInstance(Clock.systemDefaultZone());

                bind(String.class).annotatedWith(CompControlApiKey.class).toInstance("CompControlApiKey");
                bind(CompactionControlSource.class).annotatedWith(LocalCompactionControl.class).toInstance(mock(CompactionControlSource.class));

                bind(Environment.class).toInstance(mock(Environment.class));

                EmoServiceMode serviceMode = EmoServiceMode.STANDARD_ALL;
                install(new SelfHostAndPortModule());
                install(new DataCenterModule(serviceMode));
                install(new CacheManagerModule());
                install(new DataStoreModule(serviceMode));
                install(new BlobStoreModule(serviceMode, "bv.emodb.blob", new MetricRegistry()));
            }
        });
        _store = injector.getInstance(BlobStore.class);

        _lifeCycle.start();

        TableOptions options = new TableOptionsBuilder().setPlacement("media_global:ugc").build();
        Audit audit = new AuditBuilder().setLocalHost().build();
        _store.createTable(TABLE, options, ImmutableMap.<String, String>of(), audit);
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
        assertEquals(numCassandraHealthChecks, 3);  // app, ugc, media
    }

    @Test
    public void testCassandraBlobStore() throws Exception {
        String blobId = UUID.randomUUID().toString();

        // get fails initially
        verifyNotExists(blobId);

        // put some data and verify it, roughly 8MB
        verifyPutAndGet(blobId, randomBytes(0x812345), ImmutableMap.of("encoding", "image/jpeg", "name", "mycat.jpg", "owner", "clover"), Duration.ofHours(1));

        // overwrite it with a smaller blob, different data.  this isn't recommended because it will likely leave orphaned rows, but the blobstore doesn't prevent it.
        verifyPutAndGet(blobId, randomBytes(0x4321), ImmutableMap.of("encoding", "image/png", "name", "mycat2.png"), null);

        // clean up
        _store.delete(TABLE, blobId);

        // get should fail after the delete
        verifyNotExists(blobId);
    }

    private byte[] randomBytes(int length) {
        byte[] buf = new byte[length];
        _random.nextBytes(buf);
        return buf;
    }

    private void verifyNotExists(String blobId) {
        try {
            _store.get(TABLE, blobId);
            fail();
        } catch (BlobNotFoundException e) {
            // expected
        }
        try {
            _store.get(TABLE, blobId);
            fail();
        } catch (BlobNotFoundException e) {
            // expected
        }
    }

    private void verifyPutAndGet(String blobId, final byte[] blobData, Map<String, String> attributes, @Nullable Duration ttl)
            throws IOException {
        _store.put(TABLE, blobId, new InputSupplier<InputStream>() {
            @Override
            public InputStream getInput() throws IOException {
                return new ByteArrayInputStream(blobData);
            }
        }, attributes, ttl);

        // verify that we can get what we put
        Blob blob = _store.get(TABLE, blobId);
        assertEquals(blob.getId(), blobId);
        assertEquals(blob.getLength(), blobData.length);
        assertEquals(blob.getAttributes(), attributes);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        blob.writeTo(buf);
        assertEquals(buf.toByteArray(), blobData);

        BlobMetadata md = _store.getMetadata(TABLE, blobId);
        assertEquals(md.getId(), blobId);
        assertEquals(md.getLength(), blobData.length);
        assertEquals(md.getAttributes(), attributes);
    }

    @Test
    public void testTableAttributes() {
        Audit audit = new AuditBuilder().setLocalHost().build();

        // @BeforeClass creates a table with empty attributes.
        assertEquals(_store.getTableAttributes(TABLE), ImmutableMap.of());

        // Test setTableAttributes with a new attributes.
        Map<String, String> template = ImmutableMap.of("hello", "world", "unicode", "pi=\"\u03a0\"");
        _store.setTableAttributes(TABLE, template, audit);
        assertEquals(_store.getTableAttributes(TABLE), template);

        // Null values should be rejected
        Map<String, String> attributes = Maps.newHashMap();
        attributes.put("null", null);
        try {
            _store.setTableAttributes(TABLE, attributes, audit);
            fail();
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // Non-String values should be rejected.
        try {
            //noinspection unchecked
            _store.setTableAttributes(TABLE, (Map) ImmutableMap.of("num", 5), audit);
            fail();
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // Restore back to an empty attributes.
        _store.setTableAttributes(TABLE, ImmutableMap.<String, String>of(), audit);
        assertEquals(_store.getTableAttributes(TABLE), ImmutableMap.of());
    }

    @Test
    public void testListTables() {
        Iterator<Table> tableIter;

        tableIter = _store.listTables(null, Long.MAX_VALUE);
        boolean found = false;
        while (tableIter.hasNext()) {
            Table table = tableIter.next();
            assertTrue(!table.getName().startsWith("__")); // No internal tables
            if (TABLE.equals(table.getName())) {
                assertEquals(table.getOptions(), new TableOptionsBuilder().setPlacement("media_global:ugc").build());
                assertEquals(table.getAttributes(), ImmutableMap.of());
                found = true;
            }
        }
        assertTrue(found);

        tableIter = _store.listTables(TABLE, Long.MAX_VALUE);
        while (tableIter.hasNext()) {
            assertNotEquals(tableIter.next().getName(), TABLE);
        }
    }
}