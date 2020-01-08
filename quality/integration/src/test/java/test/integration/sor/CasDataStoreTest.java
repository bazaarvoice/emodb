package test.integration.sor;

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
import com.bazaarvoice.emodb.common.json.JsonValidationException;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.ReplicationKey;
import com.bazaarvoice.emodb.datacenter.DataCenterConfiguration;
import com.bazaarvoice.emodb.datacenter.DataCenterModule;
import com.bazaarvoice.emodb.datacenter.api.KeyspaceDiscovery;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.sor.DataStoreConfiguration;
import com.bazaarvoice.emodb.sor.DataStoreModule;
import com.bazaarvoice.emodb.sor.DataStoreZooKeeper;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.Table;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.compactioncontrol.CompControlApiKey;
import com.bazaarvoice.emodb.sor.compactioncontrol.LocalCompactionControl;
import com.bazaarvoice.emodb.sor.core.SystemDataStore;
import com.bazaarvoice.emodb.sor.db.cql.CqlForMultiGets;
import com.bazaarvoice.emodb.sor.db.cql.CqlForScans;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.table.db.consistency.GlobalFullConsistencyZooKeeper;
import com.bazaarvoice.emodb.web.util.ZKNamespaces;
import com.bazaarvoice.ostrich.ServiceRegistry;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.server.ServerFactory;
import io.dropwizard.server.SimpleServerFactory;
import io.dropwizard.setup.Environment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.validation.Validation;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class CasDataStoreTest {
    private static final String TABLE = "test" + UUID.randomUUID().toString();

    private SimpleLifeCycleRegistry _lifeCycle;
    private HealthCheckRegistry _healthChecks;
    private DataStore _store;

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

        // Setup the DataStoreModule
        Injector injector = Guice.createInjector(new AbstractModule() {
            @Override
            protected void configure() {
                bind(LifeCycleRegistry.class).toInstance(_lifeCycle);
                bind(HealthCheckRegistry.class).toInstance(_healthChecks);
                bind(TaskRegistry.class).toInstance(mock(TaskRegistry.class));

                DataStoreConfiguration dataStoreConfiguration = new DataStoreConfiguration()
                        .setValidTablePlacements(ImmutableSet.of("app_global:sys", "ugc_global:ugc"))
                        .setCassandraClusters(ImmutableMap.<String, CassandraConfiguration>of(
                                "ugc_global", new TestCassandraConfiguration("ugc_global", "ugc_delta_v2"),
                                "app_global", new TestCassandraConfiguration("app_global", "sys_delta_v2")))
                        .setHistoryTtl(Duration.ofDays(2));

                bind(DataStoreConfiguration.class).toInstance(dataStoreConfiguration);

                bind(String.class).annotatedWith(SystemTablePlacement.class).toInstance("app_global:sys");

                bind(DataStore.class).annotatedWith(SystemDataStore.class).toInstance(mock(DataStore.class));
                bind(JobService.class).toInstance(mock(JobService.class));
                bind(JobHandlerRegistry.class).toInstance(mock(JobHandlerRegistry.class));

                bind(DataCenterConfiguration.class).toInstance(new DataCenterConfiguration()
                        .setCurrentDataCenter("datacenter1")
                        .setSystemDataCenter("datacenter1")
                        .setDataCenterServiceUri(URI.create("http://localhost:8080"))
                        .setDataCenterAdminUri(URI.create("http://localhost:8080")));

                bind(CqlDriverConfiguration.class).toInstance(new CqlDriverConfiguration());

                bind(KeyspaceDiscovery.class).annotatedWith(Names.named("blob")).toInstance(mock(KeyspaceDiscovery.class));
                bind(String.class).annotatedWith(ServerCluster.class).toInstance("local_default");

                bind(String.class).annotatedWith(ReplicationKey.class).toInstance("password");
                bind(String.class).annotatedWith(InvalidationService.class).toInstance("emodb-cachemgr");

                bind(CuratorFramework.class).annotatedWith(Global.class).toInstance(curator);
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

                bind(Environment.class).toInstance(new Environment("emodb", Jackson.newObjectMapper(),
                        Validation.buildDefaultValidatorFactory().getValidator(),
                        new MetricRegistry(), ClassLoader.getSystemClassLoader()));

                EmoServiceMode serviceMode = EmoServiceMode.STANDARD_ALL;
                install(new SelfHostAndPortModule());
                install(new DataCenterModule(serviceMode));
                install(new CacheManagerModule());
                install(new DataStoreModule(serviceMode));
            }
        });
        _store = injector.getInstance(DataStore.class);
        _lifeCycle.start();

        Map<String, Object> template = Collections.emptyMap();
        _store.createTable(TABLE, new TableOptionsBuilder().setPlacement("ugc_global:ugc").build(), template, newAudit("create table"));
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
        assertEquals(numCassandraHealthChecks, 2);  // app, ugc
    }

    @Test
    public void testCassandraDataStore() throws Exception {
        Date start = new Date();

        String key1 = UUID.randomUUID().toString();
        String key2 = UUID.randomUUID().toString();
        String key3 = UUID.randomUUID().toString();
        String key4 = UUID.randomUUID().toString();

        ReadConsistency rSTRONG = ReadConsistency.STRONG;
        WriteConsistency wSTRONG = WriteConsistency.STRONG;

        _store.update(TABLE, key1, TimeUUIDs.newUUID(), Deltas.literal(ImmutableMap.of("name", "Bob")), newAudit("submit"), wSTRONG);
        _store.update(TABLE, key1, TimeUUIDs.newUUID(), Deltas.mapBuilder().put("state", "SUBMITTED").build(), newAudit("begin moderation"), wSTRONG);
        _store.update(TABLE, key2, TimeUUIDs.newUUID(), Deltas.literal(ImmutableMap.of("name", "Joe")), newAudit("submit"), wSTRONG);
        _store.update(TABLE, key1, TimeUUIDs.newUUID(), Deltas.mapBuilder().put("state", "APPROVED").build(), newAudit("finish moderation"), wSTRONG);

        _store.update(TABLE, key4, TimeUUIDs.newUUID(), Deltas.fromString("{\"name\":\"Alice\"}"), newAudit("submit"), WriteConsistency.STRONG);
        _store.updateAll(Collections.singleton(new Update(TABLE, key4, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"state\":\"SUBMITTED\"}"),
                newAudit("begin moderation"), WriteConsistency.STRONG)), ImmutableSet.of("submit"));
        _store.update(TABLE, key4, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"state\":\"APPROVED\"}"), newAudit("finish moderation"), WriteConsistency.STRONG);
        _store.update(TABLE, key4, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"delta1\":\"value1\"}"), newAudit("finish moderation"), WriteConsistency.STRONG);
        _store.update(TABLE, key4, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"delta2\":\"value2\"}"), newAudit("finish moderation"), WriteConsistency.STRONG);
        _store.update(TABLE, key4, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"delta3\":\"value3\"}"), newAudit("finish moderation"), WriteConsistency.STRONG);
        _store.update(TABLE, key4, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"delta4\":\"value4\"}"), newAudit("finish moderation"), WriteConsistency.STRONG);
        _store.update(TABLE, key4, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"delta5\":\"value5\"}"), newAudit("finish moderation"), WriteConsistency.STRONG);
        _store.update(TABLE, key4, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"delta6\":\"value6\"}"), newAudit("finish moderation"), WriteConsistency.STRONG);
        _store.update(TABLE, key4, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"delta7\":\"value7\"}"), newAudit("finish moderation"), WriteConsistency.STRONG);
        _store.update(TABLE, key4, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"delta8\":\"value8\"}"), newAudit("finish moderation"), WriteConsistency.STRONG);
        _store.updateAll(Collections.singleton(new Update(TABLE, key4, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"delta9\":\"value9\"}"), newAudit("finish moderation"),
                WriteConsistency.STRONG)), ImmutableSet.of("moderate", "done"));
        _store.update(TABLE, key4, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"delta10\":\"value10\"}"), newAudit("finish moderation"), WriteConsistency.STRONG);
        _store.update(TABLE, key4, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"delta11\":\"value11\"}"), newAudit("finish moderation"), WriteConsistency.STRONG);

        Date end = new Date(System.currentTimeMillis() + 1000);  // TimeUUID generation may result in dates that are too new by a very small amount

        // verify everything we have written about key1
        Map<String, Object> content1 = _store.get(TABLE, key1, rSTRONG);
        ImmutableMap<Object, Object> content1Expected = ImmutableMap.builder().
                put("~id", key1).
                put("~table", TABLE).
                put("~version", 3L).
                put("~deleted", false).
                put("~signature", Intrinsic.getSignature(content1)).
                put("~firstUpdateAt", content1.get(Intrinsic.FIRST_UPDATE_AT)).
                put("~lastUpdateAt", content1.get(Intrinsic.LAST_UPDATE_AT)).
                put("~lastMutateAt", content1.get(Intrinsic.LAST_MUTATE_AT)).
                put("name", "Bob").
                put("state", "APPROVED").
                build();
        assertEquals(content1, content1Expected);
        assertTrue(start.compareTo(Intrinsic.getFirstUpdateAt(content1)) <= 0);
        assertTrue((Intrinsic.getFirstUpdateAt(content1)).compareTo(Intrinsic.getLastUpdateAt(content1)) <= 0);
        assertTrue(end.compareTo(Intrinsic.getLastUpdateAt(content1)) >= 0);

        // verify everything we have written about key2
        Map<String, Object> content2 = _store.get(TABLE, key2, rSTRONG);
        assertEquals(content2, ImmutableMap.builder().
                put("~id", key2).
                put("~table", TABLE).
                put("~version", 1L).
                put("~deleted", false).
                put("~signature", Intrinsic.getSignature(content2)).
                put("~firstUpdateAt", content2.get(Intrinsic.FIRST_UPDATE_AT)).
                put("~lastUpdateAt", content2.get(Intrinsic.LAST_UPDATE_AT)).
                put("~lastMutateAt", content2.get(Intrinsic.LAST_MUTATE_AT)).
                put("name", "Joe").
                build());

        // verify everything we have written about key3
        Map<String, Object> content3 = _store.get(TABLE, key3, rSTRONG);
        assertEquals(content3, ImmutableMap.builder().
                put("~id", key3).
                put("~table", TABLE).
                put("~version", 0L).
                put("~deleted", true).
                put("~signature", "00000000000000000000000000000000").
                build());

        // verify everything we have written about key4
        Map<String, Object> content4 = _store.get(TABLE, key4, ReadConsistency.STRONG);
        ImmutableMap<Object, Object> content4Expected = ImmutableMap.builder().
                put("~id", key4).
                put("~table", TABLE).
                put("~version", 14L).
                put("~deleted", false).
                put("~signature", Intrinsic.getSignature(content4)).
                put("~firstUpdateAt", content4.get(Intrinsic.FIRST_UPDATE_AT)).
                put("~lastUpdateAt", content4.get(Intrinsic.LAST_UPDATE_AT)).
                put("~lastMutateAt", content4.get(Intrinsic.LAST_MUTATE_AT)).
                put("name", "Alice").
                put("state", "APPROVED").
                put("delta1", "value1").
                put("delta2", "value2").
                put("delta3", "value3").
                put("delta4", "value4").
                put("delta5", "value5").
                put("delta6", "value6").
                put("delta7", "value7").
                put("delta8", "value8").
                put("delta9", "value9").
                put("delta10", "value10").
                put("delta11", "value11").
                build();
        assertEquals(content4, content4Expected);
        assertTrue(Intrinsic.getSignature(content4).matches("[0-9a-f]{32}"));
        assertTrue(start.compareTo(Intrinsic.getFirstUpdateAt(content4)) <= 0);
        assertTrue((Intrinsic.getFirstUpdateAt(content4)).compareTo(Intrinsic.getLastUpdateAt(content4)) <= 0);
        assertTrue(end.compareTo(Intrinsic.getLastUpdateAt(content4)) >= 0);

        // try compaction with multiple threads to compact key3. This will surface race condition issues, even though it is inconsistent.
        try {
            multiThreadCompactionTest(content4Expected, key4);
        } catch (InterruptedException | ExecutionException e) {
            throw new Exception("This test is specifically designed to catch race conditions. So, please do not ignore any intermittent failures, as it points to a race condition error", e);
        } catch (Exception e) {
            throw new Exception("This test is specifically designed to catch race conditions. So, please do not ignore any intermittent failures, as it points to a race condition error", e);
        }

        // now delete key2 and verify that the delete has the expected effect
        _store.update(TABLE, key2, TimeUUIDs.newUUID(), Deltas.delete(), newAudit("delete"), wSTRONG);
        Map<String, Object> content2Deleted = _store.get(TABLE, key2, rSTRONG);
        ImmutableMap<Object, Object> content2DeletedExpected = ImmutableMap.builder().
                put("~id", key2).
                put("~table", TABLE).
                put("~version", 2L).
                put("~deleted", true).
                put("~signature", Intrinsic.getSignature(content2Deleted)).
                put("~firstUpdateAt", content2.get(Intrinsic.FIRST_UPDATE_AT)).
                put("~lastUpdateAt", content2Deleted.get(Intrinsic.LAST_UPDATE_AT)).
                put("~lastMutateAt", content2Deleted.get(Intrinsic.LAST_MUTATE_AT)).
                build();
        assertEquals(content2Deleted, content2DeletedExpected);
        assertTrue((Intrinsic.getLastUpdateAt(content2Deleted)).compareTo(Intrinsic.getLastUpdateAt(content2)) >= 0);

        // try to compact key1 with a long "full-consistency" ttl.  this should have no effect because no deltas are old enough to compact.
        _store.compact(TABLE, key1, null, rSTRONG, wSTRONG);
        assertEquals(getDeltas(_store.getTimeline(TABLE, key1, true, false, null, null, false, 100, rSTRONG)).size(), 3);

        // try again to compact key1, this time assuming full consistency has been achieved.  verify compaction doesn't change the content.
        _store.compact(TABLE, key1, Duration.ZERO, rSTRONG, wSTRONG);
        // The deletion of deltas are deferred until compaction is behind FCT. So, we would still see all 3 deltas
        assertEquals(getDeltas(_store.getTimeline(TABLE, key1, true, false, null, null, false, 100, rSTRONG)).size(), 3);
        assertEquals(getCompactions(_store.getTimeline(TABLE, key1, true, false, null, null, false, 100, rSTRONG)).size(), 1);
        assertEquals(_store.get(TABLE, key1, rSTRONG), content1Expected);
        // Now compact again with full consistency, and make sure there are no deltas left.
        _store.compact(TABLE, key1, Duration.ZERO, rSTRONG, wSTRONG);
        assertEquals(getDeltas(_store.getTimeline(TABLE, key1, true, false, null, null, false, 100, rSTRONG)).size(), 0);
        assertEquals(getCompactions(_store.getTimeline(TABLE, key1, true, false, null, null, false, 100, rSTRONG)).size(), 1);


        // try to compact key2 (which was deleted) with a long "full-consistency" ttl.  this should have no effect because no deltas are old enough to compact.
        _store.compact(TABLE, key2, null, rSTRONG, wSTRONG);
        assertEquals(getDeltas(_store.getTimeline(TABLE, key2, true, false, null, null, false, 100, rSTRONG)).size(), 2);
        assertEquals(_store.get(TABLE, key2, rSTRONG), content2DeletedExpected);
    }

    private void multiThreadCompactionTest(ImmutableMap<Object, Object> contentExpected, final String key4)
            throws InterruptedException, ExecutionException {
        Callable<Boolean> task = new Callable<Boolean>() {
            @Override
            public Boolean call() {
                _store.compact(TABLE, key4, Duration.ZERO, ReadConsistency.STRONG, WriteConsistency.STRONG);
                return Boolean.TRUE;
            }
        };
        int threadCount = 20;
        List<Callable<Boolean>> tasks = Collections.nCopies(threadCount, task);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<Boolean>> futures = executorService.invokeAll(tasks);
        // Check for exceptions
        for (Future<Boolean> future : futures) {
            future.get(); // Throws an exception if an exception was thrown by the task.
        }
        assertEquals(threadCount, futures.size(), "problem");

        // All deltas are compacted at this point
        // Since we now defer deletes for later, the absence of deltas cannot be guaranteed even if they are "compacted".
        // They get deleted when their corresponding compaction is also behind FCT.
        assertEquals(_store.get(TABLE, key4, ReadConsistency.STRONG), contentExpected);
        // This last non-concurrent compaction, should clear all deltas and leave the record with one compaction
        _store.compact(TABLE, key4, Duration.ZERO, ReadConsistency.STRONG, WriteConsistency.STRONG);
        assertEquals(getDeltas(_store.getTimeline(TABLE, key4, true, false, null, null, false, 100, ReadConsistency.STRONG)).size(), 0);
        assertEquals(getCompactions(_store.getTimeline(TABLE, key4, true, false, null, null, false, 100, ReadConsistency.STRONG)).size(), 1);
        assertEquals(_store.get(TABLE, key4, ReadConsistency.STRONG), contentExpected);
    }

    @Test
    public void testTableTemplate() {
        // @BeforeClass creates a table with an empty template.
        assertEquals(_store.getTableTemplate(TABLE), ImmutableMap.of());

        // Test setTableTemplate with a very simple new template.
        _store.setTableTemplate(TABLE, ImmutableMap.<String, Object>of("hello", "world"), newAudit("template"));
        assertEquals(_store.getTableTemplate(TABLE), ImmutableMap.of("hello", "world"));

        // Test various JSON types.
        Map<String, Object> template = Maps.newHashMap();
        template.put("null", null);
        template.put("false", false);
        template.put("true", true);
        template.put("string", "pi=\"\u03a0\"");
        template.put("empty", "");
        template.put("int", 5);
        template.put("long", Long.MIN_VALUE);
        template.put("double", 3.141592653589793238462643383279502884197169399375105820974944592307816406286);
        template.put("list", ImmutableList.of(1, 2L * Integer.MAX_VALUE, "three"));
        template.put("map", ImmutableMap.of("x", 1, "y", "two"));
        _store.setTableTemplate(TABLE, template, newAudit("template"));
        assertEquals(_store.getTableTemplate(TABLE), template);

        // Non-JSON should be rejected.
        try {
            _store.setTableTemplate(TABLE, ImmutableMap.<String, Object>of("set", ImmutableSet.of("one", "two")), newAudit("template"));
            fail();
        } catch (JsonValidationException e) {
            // Expected
        }

        try {
            _store.setTableTemplate("__system_sor:table", ImmutableMap.<String, Object>of("key", "value"), newAudit("template"));
            fail();
        } catch (IllegalArgumentException e) {
            // Expected
        }

        // Restore back to an empty template.
        _store.setTableTemplate(TABLE, ImmutableMap.<String, Object>of(), newAudit("template"));
        assertEquals(_store.getTableTemplate(TABLE), ImmutableMap.of());
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
                assertEquals(table.getOptions(), new TableOptionsBuilder().setPlacement("ugc_global:ugc").build());
                assertEquals(table.getTemplate(), ImmutableMap.of());
                found = true;
            }
        }
        assertTrue(found);

        tableIter = _store.listTables(TABLE, Long.MAX_VALUE);
        while (tableIter.hasNext()) {
            assertNotEquals(tableIter.next().getName(), TABLE);
        }
    }

    private Audit newAudit(String comment) {
        return new AuditBuilder().
                setProgram("test").
                setLocalHost().
                setComment(comment).
                build();
    }

    private List<Delta> getDeltas(Iterator<Change> changeIter) {
        List<Delta> deltas = Lists.newArrayList();
        while (changeIter.hasNext()) {
            Change change = changeIter.next();
            if (change.getDelta() != null) {
                deltas.add(change.getDelta());
            }
        }
        return deltas;
    }

    private List<Compaction> getCompactions(Iterator<Change> changeIter) {
        List<Compaction> compactions = Lists.newArrayList();
        while (changeIter.hasNext()) {
            Change change = changeIter.next();
            if (change.getCompaction() != null) {
                compactions.add(change.getCompaction());
            }
        }
        return compactions;
    }
}
