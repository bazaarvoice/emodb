package test.integration.blob;

import com.bazaarvoice.emodb.blob.BlobStoreConfiguration;
import com.bazaarvoice.emodb.blob.BlobStoreModule;
import com.bazaarvoice.emodb.blob.BlobStoreZooKeeper;
import com.bazaarvoice.emodb.blob.api.Blob;
import com.bazaarvoice.emodb.blob.api.BlobMetadata;
import com.bazaarvoice.emodb.blob.api.BlobNotFoundException;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.DefaultBlobMetadata;
import com.bazaarvoice.emodb.blob.api.Range;
import com.bazaarvoice.emodb.blob.api.Table;
import com.bazaarvoice.emodb.blob.core.SystemBlobStore;
import com.bazaarvoice.emodb.cachemgr.CacheManagerModule;
import com.bazaarvoice.emodb.cachemgr.invalidate.InvalidationService;
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
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.server.SimpleServerFactory;
import io.dropwizard.setup.Environment;
import org.apache.commons.codec.binary.Hex;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.stream.StreamSupport;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class CasBlobStoreTest {

    private static final String TABLE = "test" + UUID.randomUUID();
    private static final String TABLE_PLACEMENT = "media_global:ugc";

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
                        .setValidTablePlacements(ImmutableSet.of(TABLE_PLACEMENT))
                        .setCassandraClusters(ImmutableMap.of(
                                "media_global", new TestCassandraConfiguration("media_global", "ugc_blob"))));

                DataStoreConfiguration dataStoreConfiguration = new DataStoreConfiguration()
                        .setValidTablePlacements(ImmutableSet.of("app_global:sys", "ugc_global:ugc"))
                        .setCassandraClusters(ImmutableMap.of(
                                "ugc_global", new TestCassandraConfiguration("ugc_global", "ugc_delta_v2"),
                                "app_global", new TestCassandraConfiguration("app_global", "sys_delta_v2")))
                        .setHistoryTtl(Duration.ofDays(2));

                bind(DataStoreConfiguration.class).toInstance(dataStoreConfiguration);

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
        TableOptions options = new TableOptionsBuilder().setPlacement(TABLE_PLACEMENT).build();
        Audit audit = new AuditBuilder().setLocalHost().build();
        _store.createTable(TABLE, options, ImmutableMap.of(), audit);
    }

    @BeforeMethod
    public void beforeMethod() {
        _store.purgeTableUnsafe(TABLE, new AuditBuilder().setLocalHost().build());
    }

    @AfterClass
    public void teardown() throws Exception {
        _lifeCycle.stop();
    }

    @Test
    public void testHealthCheck() throws Exception {
        ArgumentCaptor<HealthCheck> captor = ArgumentCaptor.forClass(HealthCheck.class);
        verify(_healthChecks, atLeastOnce()).addHealthCheck(anyString(), captor.capture());
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
    public void testTableExisting () {
        assertTrue(_store.getTableExists(TABLE));
        assertFalse(_store.getTableExists(TABLE + 1));
    }

    @Test
    public void testIsTableAvailable () {
        assertTrue(_store.isTableAvailable(TABLE));
    }

    @Test
    public void testIsCreatedTableEmpty () {
        Iterator<BlobMetadata> iterator = _store.scanMetadata(TABLE, null, Long.MAX_VALUE);
        assertEquals(Iterators.size(iterator), 0);
    }

    @Test
    public void testGetTablePlacements () {
        assertEquals(_store.getTablePlacements(), Sets.newHashSet(TABLE_PLACEMENT));
    }

    @Test(expectedExceptions = BlobNotFoundException.class)
    public void testDeleteNotExistingBlob() throws Exception {
        final String blobId = UUID.randomUUID().toString();

        // get fails initially
        verifyBlobNotExists(blobId);
        verifyMetadataNotExists(blobId);

        _store.delete(TABLE, blobId);
    }

    @Test
    public void testCassandraBlobStore() throws Exception {
        String blobId = UUID.randomUUID().toString();

        // get fails initially
        verifyBlobNotExists(blobId);
        verifyMetadataNotExists(blobId);

        // put some data and verify it, roughly 8MB
        verifyPutAndGet(blobId, randomBytes(0x812345), ImmutableMap.of("encoding", "image/jpeg", "name", "mycat.jpg", "owner", "clover"));

        // overwrite it with a smaller blob, different data.  this isn't recommended because it will likely leave orphaned rows, but the blobstore doesn't prevent it.
        verifyPutAndGet(blobId, randomBytes(0x4321), ImmutableMap.of("encoding", "image/png", "name", "mycat2.png"));

        // clean up
        _store.delete(TABLE, blobId);

        // get should fail after the delete
        verifyBlobNotExists(blobId);
        verifyMetadataNotExists(blobId);
    }

    @Test
    public void testGetRange() throws Exception {
        String blobId = UUID.randomUUID().toString();

        // get fails initially
        verifyBlobNotExists(blobId);
        verifyMetadataNotExists(blobId);

        // putBlob some data and verify it, roughly 8MB
        byte[] blobData = randomBytes(0x812345);
        ImmutableMap<String, String> attributes = ImmutableMap.of("encoding", "image/jpeg", "name", "mycat.jpg", "owner", "clover");
        putBlob(blobId, blobData, attributes);

        Blob blob = _store.get(TABLE, blobId, blobLength -> Range.satisfiableRange(0, blobLength));
        verifyBlob(blob, blobId, blobData, attributes);
    }

    private byte[] randomBytes(int length) {
        byte[] buf = new byte[length];
        _random.nextBytes(buf);
        return buf;
    }

    private void verifyBlobNotExists(String blobId) {
        try {
            _store.get(TABLE, blobId);
            fail();
        } catch (BlobNotFoundException e) {
            // expected
        }
    }

    private void verifyMetadataNotExists(String blobId) {
        try {
            _store.getMetadata(TABLE, blobId);
            fail();
        } catch (BlobNotFoundException e) {
            // expected
        }
    }

    private void verifyPutAndGet(String blobId, final byte[] blobData, ImmutableMap<String, String> attributes)
            throws IOException {
        putBlob(blobId, blobData, attributes);

        Blob blob = _store.get(TABLE, blobId);
        // verify that we can get what we putBlob
        verifyBlob(blob, blobId, blobData, attributes);

        BlobMetadata blobMetadata = _store.getMetadata(TABLE, blobId);

        //we don't check date, so can pass any non null value
        BlobMetadata expectedBlobMetadata = getBlobMetadata(blobId, blobData, new Date(), attributes);
        assertEqualsBlobMetadata(blobMetadata, expectedBlobMetadata);
    }

    private static void verifyBlob(Blob blob, String blobId, byte[] blobData, Map<String, String> attributes) throws IOException {
        assertEquals(blob.getId(), blobId);
        assertEquals(blob.getLength(), blobData.length);
        assertEquals(blob.getAttributes(), attributes);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        blob.writeTo(buf);
        assertEquals(buf.toByteArray(), blobData);
    }

    private static void assertEqualsBlobMetadata(BlobMetadata actual, BlobMetadata expected) {
        assertEquals(actual.getId(), expected.getId());
        assertEquals(actual.getMD5(), expected.getMD5());
        assertEquals(actual.getSHA1(), expected.getSHA1());
        assertEquals(actual.getLength(), expected.getLength());
        assertEquals(actual.getAttributes(), expected.getAttributes());
    }

    private void putBlob(String blobId, byte[] blobData, Map<String, String> attributes) throws IOException {
        _store.put(TABLE, blobId, () -> new ByteArrayInputStream(blobData), attributes);
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
        _store.setTableAttributes(TABLE, ImmutableMap.of(), audit);
        assertEquals(_store.getTableAttributes(TABLE), ImmutableMap.of());
    }

    @Test
    public void testTableOptions() {
        assertEquals(_store.getTableOptions(TABLE), new TableOptionsBuilder().setPlacement(TABLE_PLACEMENT).build());
    }

    @Test(expectedExceptions = UnknownTableException.class)
    public void testNotExistingTableOptions() {
        assertEquals(_store.getTableOptions(TABLE + 1), new TableOptionsBuilder().setPlacement(TABLE_PLACEMENT).build());
    }

    @Test
    public void testListTables() {
        Iterator<Table> tableIter;

        tableIter = _store.listTables(null, Long.MAX_VALUE);
        boolean found = false;
        while (tableIter.hasNext()) {
            Table table = tableIter.next();
            assertFalse(table.getName().startsWith("__")); // No internal tables
            if (TABLE.equals(table.getName())) {
                assertEquals(table.getOptions(), new TableOptionsBuilder().setPlacement(TABLE_PLACEMENT).build());
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

    @Test
    public void testScanMetadata() throws Exception {
        String blobId1 = "1";
        String blobId2 = "2";

        assertEquals(Iterators.size(_store.scanMetadata(TABLE, null, Long.MAX_VALUE)), 0);

        verifyBlobNotExists(blobId1);
        verifyMetadataNotExists(blobId1);

        verifyBlobNotExists(blobId2);
        verifyMetadataNotExists(blobId2);
        Date now = new Date();

        ImmutableMap<String, String> attributes1 = ImmutableMap.of("encoding", "image/jpeg", "name", "mycat.jpg", "owner", "clover");
        byte[] blobData1 = randomBytes(0x812345);
        verifyPutAndGet(blobId1, blobData1, attributes1);

        ImmutableMap<String, String> attributes2 = ImmutableMap.of("encoding", "image/png", "name", "mycat2.png");
        byte[] blobData2 = randomBytes(0x4321);
        verifyPutAndGet(blobId2, blobData2, attributes2);

        BlobMetadata blobMetadata1 = getBlobMetadata(blobId1, blobData1, now, attributes1);
        BlobMetadata blobMetadata2 = getBlobMetadata(blobId2, blobData2, now, attributes2);

        assertEqualsBlobMetadata(sortIterator(_store.scanMetadata(TABLE, null, Long.MAX_VALUE)), Lists.newArrayList(blobMetadata1, blobMetadata2).iterator());
        assertEquals(Iterators.size(_store.scanMetadata(TABLE, null, 1)), 1);
    }

    private static Iterator<BlobMetadata> sortIterator(final Iterator<BlobMetadata> iterator) {
        return StreamSupport
                .stream(Spliterators
                        .spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .sorted(Comparator.comparing(BlobMetadata::getId))
                .iterator();
    }

    @Test
    public void testScanMetadataFromBlobIdExclusive() throws Exception {
        String blobId = "1";

        verifyBlobNotExists(blobId);
        verifyMetadataNotExists(blobId);
        Date now = new Date();

        ImmutableMap<String, String> attributes = ImmutableMap.of("encoding", "image/jpeg", "name", "mycat.jpg", "owner", "clover");
        byte[] blobData = randomBytes(0x812345);
        verifyPutAndGet(blobId, blobData, attributes);

        BlobMetadata blobMetadata = getBlobMetadata(blobId, blobData, now, attributes);

        assertEqualsBlobMetadata(_store.scanMetadata(TABLE, blobId, Long.MAX_VALUE), Collections.emptyIterator());
        assertEqualsBlobMetadata(_store.scanMetadata(TABLE, null, Long.MAX_VALUE), Lists.newArrayList(blobMetadata).iterator());
    }

    private static void assertEqualsBlobMetadata(Iterator<BlobMetadata> actual, Iterator<BlobMetadata> expected) {
        if (actual != expected) {
            if (actual == null || expected == null) {
                fail("Iterators not equal: expected: " + expected + " and actual: " + actual);
            }

            while(actual.hasNext() && expected.hasNext()) {
                BlobMetadata e = expected.next();
                BlobMetadata a = actual.next();
                assertEqualsBlobMetadata(a, e);
            }

            if (actual.hasNext()) {
                fail("Actual iterator returned more elements than the expected iterator.");
            } else if (expected.hasNext()) {
                fail("Expected iterator returned more elements than the actual iterator.");
            }
        }
    }

    private static BlobMetadata getBlobMetadata(String blobId, byte[] blobData, Date date, ImmutableMap<String, String> attributes) {
        MessageDigest mdMD5 = getMessageDigest("MD5");
        mdMD5.update(blobData);
        String md5 = Hex.encodeHexString(mdMD5.digest());

        MessageDigest mdSHA1 = getMessageDigest("SHA-1");
        mdSHA1.update(blobData);
        String sha1 = Hex.encodeHexString(mdSHA1.digest());

        return new DefaultBlobMetadata(blobId, date, blobData.length, md5, sha1, attributes);
    }

    private static MessageDigest getMessageDigest(String algorithmName) {
        try {
            return MessageDigest.getInstance(algorithmName);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
