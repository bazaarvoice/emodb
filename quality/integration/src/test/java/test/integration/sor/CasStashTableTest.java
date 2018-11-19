package test.integration.sor;

import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.common.cassandra.CassandraFactory;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.cassandra.test.TestCassandraConfiguration;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.bazaarvoice.emodb.sor.db.astyanax.DeltaPlacementFactory;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxTableDAO;
import com.bazaarvoice.emodb.table.db.astyanax.CQLStashTableDAO;
import com.bazaarvoice.emodb.table.db.astyanax.DataCopyDAO;
import com.bazaarvoice.emodb.table.db.astyanax.DataPurgeDAO;
import com.bazaarvoice.emodb.table.db.astyanax.FullConsistencyTimeProvider;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementFactory;
import com.bazaarvoice.emodb.table.db.astyanax.RateLimiterCache;
import com.bazaarvoice.emodb.table.db.stash.StashTokenRange;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.astyanax.partitioner.BOP20Partitioner;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.curator.framework.CuratorFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class CasStashTableTest  {

    private SimpleLifeCycleRegistry _lifeCycleRegistry;
    private AstyanaxTableDAO _astyanaxTableDAO;
    private InMemoryDataStore _tableBackingStore;
    private final Token.TokenFactory _tokenFactory = ByteOrderedPartitioner.instance.getTokenFactory();

    @BeforeClass
    public void setup() throws Exception {
        DataCenter dataCenter = mock(DataCenter.class);
        when(dataCenter.getName()).thenReturn("datacenter1");

        DataCenters dataCenters = mock(DataCenters.class);
        when(dataCenters.getSelf()).thenReturn(dataCenter);
        when(dataCenters.getForKeyspace(anyString())).thenReturn(ImmutableList.of(dataCenter));
        when(dataCenters.getAll()).thenReturn(ImmutableList.of(dataCenter));

        _lifeCycleRegistry = new SimpleLifeCycleRegistry();
        HealthCheckRegistry healthCheckRegistry = mock(HealthCheckRegistry.class);
        CuratorFramework curator = mock(CuratorFramework.class);

        CassandraFactory cassandraFactory = new CassandraFactory(_lifeCycleRegistry, healthCheckRegistry,
                curator, new MetricRegistry(), Clock.systemUTC());
        Map<String, CassandraKeyspace> keyspaceMap = Maps.newHashMap();
        keyspaceMap.putAll(cassandraFactory.build(new TestCassandraConfiguration("app_global", "sys_delta")));
        keyspaceMap.putAll(cassandraFactory.build(new TestCassandraConfiguration("ugc_global", "ugc_delta")));

        PlacementFactory placementFactory = new DeltaPlacementFactory(_lifeCycleRegistry, ImmutableSet.of("app_global:sys", "ugc_global:ugc"),
               keyspaceMap, dataCenters);

        PlacementCache placementCache = new PlacementCache(placementFactory);
        CQLStashTableDAO cqlStashTableDAO = new CQLStashTableDAO("app_global:sys", placementCache, dataCenters);

        _astyanaxTableDAO = new AstyanaxTableDAO(_lifeCycleRegistry, "__system_sor", "app_global:sys",
                8, ImmutableMap.of(), placementFactory, placementCache, "datacenter1",
                mock(RateLimiterCache.class), mock(DataCopyDAO.class), mock(DataPurgeDAO.class), mock(FullConsistencyTimeProvider.class),
                mock(ValueStore.class), mock(CacheRegistry.class), ImmutableMap.of(), false, mock(Clock.class));
        _astyanaxTableDAO.setCQLStashTableDAO(cqlStashTableDAO);
        // Don't store table definitions in the actual backing store so as not to interrupt other tests.  Use a
        // private in-memory implementation.
        _tableBackingStore = new InMemoryDataStore(new MetricRegistry());
        _astyanaxTableDAO.setBackingStore(_tableBackingStore);
        
        _lifeCycleRegistry.start();
    }
    
    @AfterClass
    public void teardown() throws Exception {
        _lifeCycleRegistry.stop();
    }

    @AfterMethod
    public void clearInMemorySystemTables() throws Exception {
        // After each test wipe the in-memory set of tables from the tacking store
        Audit audit = new AuditBuilder().setComment("cleanup").build();
        _tableBackingStore.purgeTableUnsafe("__system_sor:table", audit);
        _tableBackingStore.purgeTableUnsafe("__system_sor:table_uuid", audit);
    }

    @Test
    public void testNonExistentStashReturnsEmpty() throws Exception {
        // Create a table, just to prove it won't be found
        addTable("table0", "ugc_global:ugc", 0x1000L);
        Iterator<StashTokenRange> ranges = _astyanaxTableDAO.getStashTokenRangesFromSnapshot("stash-no-snapshot", "ugc_global:ugc",
                toToken(BOP20Partitioner.MINIMUM), toToken(BOP20Partitioner.MAXIMUM));
        assertFalse(ranges.hasNext());
    }

    @Test
    public void testInterTable() throws Exception {
        // Create two tables, both surrounding the range we'll be querying but only one in the matching placement
        addTable("table0", "ugc_global:ugc", 0x2000L);
        addTable("table1", "app_global:sys", 0x2000L);

        _astyanaxTableDAO.createStashTokenRangeSnapshot("stash-inter", ImmutableSet.of("ugc_global:ugc"), Conditions.alwaysFalse());
        List<StashTokenRange> ranges = ImmutableList.copyOf(
                _astyanaxTableDAO.getStashTokenRangesFromSnapshot("stash-inter", "ugc_global:ugc",
                        toToken("10000000000000200001"), toToken("10000000000000200099")));

        Table table0 = _astyanaxTableDAO.get("table0");
        assertStashTokenRangesMatch(ranges, ImmutableList.of(new StashTokenRange(toToken("10000000000000200001"), toToken("10000000000000200099"), table0)));
    }

    @Test
    public void testStartsWithinTable() throws Exception {
        // Create two tables, both starting within the range we'll be querying but only one in the matching placement
        addTable("table2", "ugc_global:ugc", 0x3000L);
        addTable("table3", "app_global:sys", 0x3000L);

        _astyanaxTableDAO.createStashTokenRangeSnapshot("stash-starts-within", ImmutableSet.of("ugc_global:ugc"), Conditions.alwaysFalse());
        List<StashTokenRange> ranges = ImmutableList.copyOf(
                _astyanaxTableDAO.getStashTokenRangesFromSnapshot("stash-starts-within", "ugc_global:ugc",
                        toToken("10000000000000300099"), toToken("1000000000000030a0ff")));

        Table table2 = _astyanaxTableDAO.get("table2");
        assertStashTokenRangesMatch(ranges, ImmutableList.of(new StashTokenRange(toToken("10000000000000300099"), toToken("100000000000003001"), table2)));
    }

    @Test
    public void testEndsWithinTable() throws Exception {
        // Create two tables, both ending within the range we'll be querying but only one in the matching placement
        addTable("table4", "ugc_global:ugc", 0x4000L);
        addTable("table5", "app_global:sys", 0x4000L);

        _astyanaxTableDAO.createStashTokenRangeSnapshot("stash-ends-within", ImmutableSet.of("ugc_global:ugc"), Conditions.alwaysFalse());
        List<StashTokenRange> ranges = ImmutableList.copyOf(
                _astyanaxTableDAO.getStashTokenRangesFromSnapshot("stash-ends-within", "ugc_global:ugc",
                        toToken("100000000000003fff00"), toToken("10000000000000400099")));

        Table table4 = _astyanaxTableDAO.get("table4");
        assertStashTokenRangesMatch(ranges, ImmutableList.of(new StashTokenRange(toToken("100000000000004000"), toToken("10000000000000400099"), table4)));
    }

    @Test
    public void testMatchesTable() throws Exception {
        // Create two tables, both exactly matching the range we'll be querying but only one in the matching placement
        addTable("table6", "ugc_global:ugc", 0x5000L);
        addTable("table7", "app_global:sys", 0x5000L);

        _astyanaxTableDAO.createStashTokenRangeSnapshot("stash-matches", ImmutableSet.of("ugc_global:ugc"), Conditions.alwaysFalse());
        List<StashTokenRange> ranges = ImmutableList.copyOf(
                _astyanaxTableDAO.getStashTokenRangesFromSnapshot("stash-matches", "ugc_global:ugc",
                        toToken("100000000000005000"), toToken("100000000000005001")));

        Table table6 = _astyanaxTableDAO.get("table6");
        assertStashTokenRangesMatch(ranges, ImmutableList.of(new StashTokenRange(toToken("100000000000005000"), toToken("100000000000005001"), table6)));
    }

    @Test
    public void testSurroundsTable() throws Exception {
        // Create two tables, both completely within the range we'll be querying but only one in the matching placement
        addTable("table8", "ugc_global:ugc", 0x6000L);
        addTable("table9", "app_global:sys", 0x6000L);

        _astyanaxTableDAO.createStashTokenRangeSnapshot("stash-surrounds", ImmutableSet.of("ugc_global:ugc"), Conditions.alwaysFalse());
        List<StashTokenRange> ranges = ImmutableList.copyOf(
                _astyanaxTableDAO.getStashTokenRangesFromSnapshot("stash-surrounds", "ugc_global:ugc",
                        toToken("100000000000005fff"), toToken("100000000000006002")));

        Table table8 = _astyanaxTableDAO.get("table8");
        assertStashTokenRangesMatch(ranges, ImmutableList.of(new StashTokenRange(toToken("100000000000006000"), toToken("100000000000006001"), table8)));
    }

    @Test
    public void testMultipleTables() throws Exception {
        // Create multiple tables within the range we'll be querying plus one more in a non-matching placement.
        // Add them out of order to ensure they are being sorted properly and the sorting is not just a consequence
        // of them being returned in insertion order.
        addTable("table14", "ugc_global:ugc", 0x7040L);
        addTable("table12", "ugc_global:ugc", 0x7020L);
        addTable("table10", "ugc_global:ugc", 0x7000L);
        addTable("table11", "ugc_global:ugc", 0x7010L);
        addTable("table13", "ugc_global:ugc", 0x7030L);
        addTable("table15", "app_global:sys", 0x7000L);

        _astyanaxTableDAO.createStashTokenRangeSnapshot("stash-multiple", ImmutableSet.of("ugc_global:ugc"), Conditions.alwaysFalse());
        List<StashTokenRange> ranges = ImmutableList.copyOf(
                _astyanaxTableDAO.getStashTokenRangesFromSnapshot("stash-multiple", "ugc_global:ugc",
                        toToken("100000000000006fff"), toToken("100000000000007100")));

        Table table10 = _astyanaxTableDAO.get("table10");
        Table table11 = _astyanaxTableDAO.get("table11");
        Table table12 = _astyanaxTableDAO.get("table12");
        Table table13 = _astyanaxTableDAO.get("table13");
        Table table14 = _astyanaxTableDAO.get("table14");
        assertStashTokenRangesMatch(ranges, ImmutableList.of(
                new StashTokenRange(toToken("100000000000007000"), toToken("100000000000007001"), table10),
                new StashTokenRange(toToken("100000000000007010"), toToken("100000000000007011"), table11),
                new StashTokenRange(toToken("100000000000007020"), toToken("100000000000007021"), table12),
                new StashTokenRange(toToken("100000000000007030"), toToken("100000000000007031"), table13),
                new StashTokenRange(toToken("100000000000007040"), toToken("100000000000007041"), table14)));

    }

    @Test
    public void testConsecutiveTableUUIDs() throws Exception {
        // Create two tables with consecutive UUIDs
        addTable("table16", "ugc_global:ugc", 0x8000L);
        addTable("table17", "ugc_global:ugc", 0x8001L);

        _astyanaxTableDAO.createStashTokenRangeSnapshot("stash-consecutive", ImmutableSet.of("ugc_global:ugc"), Conditions.alwaysFalse());
        List<StashTokenRange> ranges = ImmutableList.copyOf(
                _astyanaxTableDAO.getStashTokenRangesFromSnapshot("stash-consecutive", "ugc_global:ugc",
                        toToken("100000000000007fff"), toToken("10000000000000800a")));

        Table table16 = _astyanaxTableDAO.get("table16");
        Table table17 = _astyanaxTableDAO.get("table17");
        assertStashTokenRangesMatch(ranges, ImmutableList.of(
                new StashTokenRange(toToken("100000000000008000"), toToken("100000000000008001"), table16),
                new StashTokenRange(toToken("100000000000008001"), toToken("100000000000008002"), table17)));

    }

    @Test
    public void testAllShardsForTable() throws Exception {
        // Create a single table
        addTable("table18", "ugc_global:ugc", 0x9000L);

        _astyanaxTableDAO.createStashTokenRangeSnapshot("stash-all-shards", ImmutableSet.of("ugc_global:ugc"), Conditions.alwaysFalse());

        // Get all possible ranges.  This should return every shard for the table.
        List<StashTokenRange> ranges = ImmutableList.copyOf(
                _astyanaxTableDAO.getStashTokenRangesFromSnapshot("stash-all-shards", "ugc_global:ugc",
                        toToken(BOP20Partitioner.MINIMUM), toToken(BOP20Partitioner.MAXIMUM)));

        Table table18 = _astyanaxTableDAO.get("table18");
        List<StashTokenRange> expected = Lists.newArrayListWithCapacity(256);
        for (int i=0; i < 256; i++){
            expected.add(new StashTokenRange(
                    toToken(String.format("%02x0000000000009000", i)),
                    toToken(String.format("%02x0000000000009001", i)),
                    table18));
        }
        assertStashTokenRangesMatch(ranges, expected);
    }

    private ByteBuffer toToken(String tokenString) {
        return _tokenFactory.toByteArray(_tokenFactory.fromString(tokenString));
    }

    private void addTable(String tableName, String placement, long uuid) {
        String uuidStr = Long.toHexString(uuid);

        // Since we need fine control over the UUIDs repeat the inserts the table DAO would normally produce here.
        // Use 256 shards to ensure that every possible shard is in use.  This makes testing simpler since there
        // is some computation involved to determine which table UUIDs will use which subset of shard IDs when using
        // less than 256.
        Delta delta = Deltas.mapBuilder()
                .put("uuid", uuidStr)
                .put("attributes", ImmutableMap.of())
                .put("storage", ImmutableMap.<String, Object>of(uuidStr,
                        ImmutableMap.<String, Object>of("placement", placement, "shards", 256)))
                .build();

        Audit audit = new AuditBuilder().setComment("test").build();
        _tableBackingStore.update("__system_sor:table", tableName, TimeUUIDs.newUUID(), delta, audit);

        delta = Deltas.mapBuilder()
                .put("table", tableName)
                .build();

        _tableBackingStore.update("__system_sor:table_uuid", uuidStr, TimeUUIDs.newUUID(), delta, audit);
    }

    private void assertStashTokenRangesMatch(Iterable<StashTokenRange> actual, Iterable<StashTokenRange> expected) {
        Iterator<StashTokenRange> actualIter = actual.iterator();
        Iterator<StashTokenRange> expectedIter = expected.iterator();

        while (actualIter.hasNext()) {
            StashTokenRange actualRange = actualIter.next();
            assertTrue(expectedIter.hasNext());
            StashTokenRange expectedRange = expectedIter.next();

            assertEquals(ByteBufferUtil.compareUnsigned(actualRange.getFrom(), expectedRange.getFrom()), 0);
            assertEquals(ByteBufferUtil.compareUnsigned(actualRange.getTo(), expectedRange.getTo()), 0);
            assertEquals(actualRange.getTable().getName(), expectedRange.getTable().getName());
        }
        assertFalse(expectedIter.hasNext());
    }
}
