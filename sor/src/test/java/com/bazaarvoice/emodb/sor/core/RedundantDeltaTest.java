package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.event.api.BaseEventStore;
import com.bazaarvoice.emodb.queue.core.kafka.KafkaProducerService;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.ChangeBuilder;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.audit.DiscardingAuditWriter;
import com.bazaarvoice.emodb.sor.compactioncontrol.InMemoryCompactionControlSource;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.test.DiscardingExecutorService;
import com.bazaarvoice.emodb.sor.core.test.InMemoryHistoryStore;
import com.bazaarvoice.emodb.sor.core.test.InMemoryMapStore;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.db.test.DeltaClusteringKey;
import com.bazaarvoice.emodb.sor.db.test.InMemoryDataReaderDAO;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.kafka.KafkaProducerService;
import com.bazaarvoice.emodb.sor.log.NullSlowQueryLog;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.test.InMemoryTableDAO;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class RedundantDeltaTest {

    private static final String TABLE = "item";
    private static final String KEY = "key1";

    @Test
    public void testRedundantDeltas() throws Exception {
        InMemoryDataReaderDAO dataDao = new InMemoryDataReaderDAO();
        DefaultDataStore store = new DefaultDataStore(new DatabusEventWriterRegistry(), new InMemoryTableDAO(), dataDao, dataDao,
                new NullSlowQueryLog(), new DiscardingExecutorService(), new InMemoryHistoryStore(),
                Optional.empty(), new InMemoryCompactionControlSource(), Conditions.alwaysFalse(),
                new DiscardingAuditWriter(), new InMemoryMapStore<>(), new MetricRegistry(), Clock.systemUTC(), new KafkaProducerService());

        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        store.createTable(TABLE, options, Collections.emptyMap(), newAudit("create table"));

        UUID uuid0 = TimeUUIDs.newUUID();
        UUID uuid1 = TimeUUIDs.newUUID();
        UUID uuid2 = TimeUUIDs.newUUID();
        UUID uuid3 = TimeUUIDs.newUUID();
        UUID uuid4 = TimeUUIDs.newUUID();
        UUID uuid5 = TimeUUIDs.newUUID();
        UUID uuid6 = TimeUUIDs.newUUID();
        UUID uuid7 = TimeUUIDs.newUUID();

        store.update(TABLE, KEY, uuid1, Deltas.fromString("{\"name\":\"Bob\"}"), newAudit("submit"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, uuid2, Deltas.fromString("{..,\"state\":\"APPROVED\"}"), newAudit("moderation"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, uuid3, Deltas.fromString("{..,\"state\":\"APPROVED\"}"), newAudit("moderation"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, uuid4, Deltas.fromString("{\"name\":\"Bob\"}"), newAudit("resubmit"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, uuid5, Deltas.fromString("{\"name\":\"Tom\"}"), newAudit("resubmit"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, uuid6, Deltas.fromString("{\"name\":\"Tom\"}"), newAudit("resubmit"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, uuid7, Deltas.fromString("{\"name\":\"Tom\"}"), newAudit("resubmit"), WriteConsistency.STRONG);
        Map<String, String> expectedFinalState = ImmutableMap.of("name", "Tom");

        assertUnknownDelta(store, TABLE, KEY, uuid0);
        assertChange(store, TABLE, KEY, uuid1, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid2, expectedFinalState);
        assertRedundantDelta(store, TABLE, KEY, uuid3);
        assertChange(store, TABLE, KEY, uuid4, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid5, expectedFinalState);
        assertRedundantDelta(store, TABLE, KEY, uuid6);
        assertRedundantDelta(store, TABLE, KEY, uuid7);
        assertUnknownDelta(store, TABLE, KEY, TimeUUIDs.newUUID());

        // automatic compaction should be disabled in this test.  verify that using DiscardingExecutorService disabled it.
        assertEquals(Iterators.size(store.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG)), 7);

        // now compact and verify that all deltas are read normally
        dataDao.setFullConsistencyDelayMillis(0);
        store.compact(TABLE, KEY, null, ReadConsistency.STRONG, WriteConsistency.STRONG);
        assertChange(store, TABLE, KEY, uuid0, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid1, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid2, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid3, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid4, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid5, expectedFinalState);
        assertRedundantDelta(store, TABLE, KEY, uuid6);
        assertRedundantDelta(store, TABLE, KEY, uuid7);
        dataDao.setFullConsistencyDelayMillis(Integer.MAX_VALUE);
        assertUnknownDelta(store, TABLE, KEY, TimeUUIDs.newUUID());
    }

    @Test
    public void testMinUUIDDelta() throws Exception {
        InMemoryDataReaderDAO dataDao = new InMemoryDataReaderDAO();
        DefaultDataStore store = new DefaultDataStore(new DatabusEventWriterRegistry(), new InMemoryTableDAO(), dataDao, dataDao,
                new NullSlowQueryLog(), new DiscardingExecutorService(), new InMemoryHistoryStore(),
                Optional.empty(), new InMemoryCompactionControlSource(), Conditions.alwaysFalse(),
                new DiscardingAuditWriter(), new InMemoryMapStore<>(), new MetricRegistry(), Clock.systemUTC(), new KafkaProducerService());

        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        store.createTable(TABLE, options, Collections.emptyMap(), newAudit("create table"));

        UUID uuid1 = TimeUUIDs.newUUID();
        UUID uuid2 = TimeUUIDs.minimumUuid();

        store.update(TABLE, KEY, uuid1, Deltas.fromString("{\"name\":\"Bob\"}"), newAudit("submit"), WriteConsistency.STRONG);

        // now try to update the delta with minUUID. This will result in a failed write, however we want to check that the resultant event to the databus
        // will not get Discarded during resolving (checks we have in resolvePeekOrPollEvents() in DefaultDatabus).
        try {
            store.update(TABLE, KEY, uuid2, Deltas.fromString("{\"name\":\"Tom\"}"), newAudit("submit"), WriteConsistency.STRONG);
        } catch (Exception e) {
            // this is expected as we have to fail with the changeId too far in the past.
        }

        // the final state is still the UUID1 delta content.
        Map<String, String> expectedFinalState = ImmutableMap.of("name", "Bob");

        assertChange(store, TABLE, KEY, uuid1, expectedFinalState);

        assertChange(store, TABLE, KEY, uuid2, expectedFinalState);

        // check explicitly for isChangeDeltaPending and isChangeRedundant checks, although above assertChange for uuid2 should also do these checks internally.
        // This means the events don't get discarded in any possible way during resolving.
        assertFalse(getAnnotated(store, TABLE, KEY, ReadConsistency.STRONG).isChangeDeltaPending(uuid2));
        assertFalse(getAnnotated(store, TABLE, KEY, ReadConsistency.STRONG).isChangeDeltaRedundant(uuid2));
    }

    @Test
    public void testRedundancyWithTags() throws Exception {
        InMemoryDataReaderDAO dataDao = new InMemoryDataReaderDAO();
        DefaultDataStore store = new DefaultDataStore(new DatabusEventWriterRegistry(), new InMemoryTableDAO(), dataDao, dataDao,
                new NullSlowQueryLog(), new DiscardingExecutorService(), new InMemoryHistoryStore(),
                Optional.empty(), new InMemoryCompactionControlSource(), Conditions.alwaysFalse(),
                new DiscardingAuditWriter(), new InMemoryMapStore<>(), new MetricRegistry(), Clock.systemUTC(), new KafkaProducerService());

        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        store.createTable(TABLE, options, Collections.emptyMap(), newAudit("create table"));

        UUID uuid0 = TimeUUIDs.newUUID();
        UUID uuid1 = TimeUUIDs.newUUID();
        UUID uuid2 = TimeUUIDs.newUUID();
        UUID uuid3 = TimeUUIDs.newUUID();
        UUID uuid4 = TimeUUIDs.newUUID();
        UUID uuid5 = TimeUUIDs.newUUID();
        UUID uuid6 = TimeUUIDs.newUUID();
        UUID uuid7 = TimeUUIDs.newUUID();
        UUID uuid8 = TimeUUIDs.newUUID();
        UUID uuid9 = TimeUUIDs.newUUID();
        UUID uuid10 = TimeUUIDs.newUUID();

        store.update(TABLE, KEY, uuid1, Deltas.fromString("{\"name\":\"Bob\",\"test\":[\"what\"]}"), newAudit("submit"), WriteConsistency.STRONG);
        store.updateAll(Collections.singleton(new Update(TABLE, KEY, uuid2, Deltas.fromString("{..,\"state\":\"APPROVED\"}"),
                newAudit("provision"), WriteConsistency.STRONG)), ImmutableSet.of("add"));
        // uuid3 - change due to difference in tags
        store.update(TABLE, KEY, uuid3, Deltas.fromString("{..,\"state\":\"APPROVED\"}"), newAudit("moderation"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, uuid4, Deltas.fromString("{\"name\":\"Bob\"}"), newAudit("resubmit"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, uuid5, Deltas.fromString("{\"name\":\"Tom\"}"), newAudit("resubmit"), WriteConsistency.STRONG);
        // uuid6 should *not* be redundant, even though the content is still the same, but the tags are not
        // Note that a literal update also loses all the previous event tags
        store.updateAll(Collections.singleton(new Update(TABLE, KEY, uuid6, Deltas.fromString("{\"name\":\"Tom\"}"),
                newAudit("resubmit"), WriteConsistency.STRONG)), ImmutableSet.of("resubmit"));
        // uuid7 should be considered redundant, both the content and the tags remain the same
        store.updateAll(Collections.singleton(new Update(TABLE, KEY, uuid7, Deltas.fromString("{\"name\":\"Tom\"}"),
                newAudit("resubmit"), WriteConsistency.STRONG)), ImmutableSet.of("resubmit"));
        // uuid8 is different by virtue of its tags
        store.updateAll(Collections.singleton(new Update(TABLE, KEY, uuid8, Deltas.fromString("{..,\"name\":\"Tom\"}"),
                newAudit("resubmit"), WriteConsistency.STRONG)), ImmutableSet.of("etl"));
        // uuid9 is redundant
        store.updateAll(Collections.singleton(new Update(TABLE, KEY, uuid9, Deltas.fromString("{..,\"name\":\"Tom\"}"),
                newAudit("resubmit"), WriteConsistency.STRONG)), ImmutableSet.of("etl"));
        // uuid10 is redundant
        store.updateAll(Collections.singleton(new Update(TABLE, KEY, uuid10, Deltas.fromString("{..,\"name\":\"Tom\"}"),
                newAudit("resubmit"), WriteConsistency.STRONG)), ImmutableSet.of("etl"));
        Map<String, Object> expectedFinalState = ImmutableMap.of("name", "Tom");

        assertUnknownDelta(store, TABLE, KEY, uuid0);
        assertChange(store, TABLE, KEY, uuid1, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid2, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid3, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid4, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid5, expectedFinalState);
        // uuid6 is a change only by virtue of its tags, not content
        assertChange(store, TABLE, KEY, uuid6, expectedFinalState);
        // uuid 7 is redundant
        assertRedundantDelta(store, TABLE, KEY, uuid7);
        assertChange(store, TABLE, KEY, uuid8, expectedFinalState);
        assertRedundantDelta(store, TABLE, KEY, uuid9);
        assertRedundantDelta(store, TABLE, KEY, uuid10);
        assertUnknownDelta(store, TABLE, KEY, TimeUUIDs.newUUID());

        // now compact and verify that all deltas are read normally
        dataDao.setFullConsistencyDelayMillis(0);
        store.compact(TABLE, KEY, null, ReadConsistency.STRONG, WriteConsistency.STRONG);
        assertChange(store, TABLE, KEY, uuid0, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid1, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid2, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid3, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid4, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid5, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid6, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid7, expectedFinalState);
        assertChange(store, TABLE, KEY, uuid8, expectedFinalState);
        assertRedundantDelta(store, TABLE, KEY, uuid9);
        assertRedundantDelta(store, TABLE, KEY, uuid10);
        dataDao.setFullConsistencyDelayMillis(Integer.MAX_VALUE);
        assertUnknownDelta(store, TABLE, KEY, TimeUUIDs.newUUID());
    }

    @Test
    public void testTagsForNestedMapDeltas() {
        InMemoryDataReaderDAO dataDao = new InMemoryDataReaderDAO();
        DefaultDataStore store = new DefaultDataStore(new DatabusEventWriterRegistry(), new InMemoryTableDAO(), dataDao, dataDao,
                new NullSlowQueryLog(), new DiscardingExecutorService(), new InMemoryHistoryStore(),
                Optional.empty(), new InMemoryCompactionControlSource(), Conditions.alwaysFalse(),
                new DiscardingAuditWriter(), new InMemoryMapStore<>(), new MetricRegistry(), Clock.systemUTC(), new KafkaProducerService());

        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        store.createTable(TABLE, options, Collections.emptyMap(), newAudit("create table"));

        // Nested deltas should only get one top-level "~tags" attribute
        store.updateAll(Collections.singleton(new Update(TABLE, KEY, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"name\":\"Bob\",\"map\":{..,\"x\":1}}"),
                newAudit("submit"), WriteConsistency.STRONG)), ImmutableSet.of("tag1","tag2"));
        store.updateAll(Collections.singleton(new Update(TABLE, KEY, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"name\":\"Bob\",\"map\":{..,\"y\":1}}"),
                newAudit("submit"), WriteConsistency.STRONG)), ImmutableSet.of("tag1","tag3"));
        Map<String, Object> expectedFinalState = ImmutableMap.of("name", "Bob", "map", ImmutableMap.<String, Object>of("x", 1, "y", 1));
        assertEquals(excludeKeys(store.get(TABLE, KEY), Intrinsic.DATA_FIELDS), expectedFinalState);
    }

    @Test
    public void testRedundancyWithCompactionAndUnchangedTag() throws Exception {
        InMemoryDataReaderDAO dataDao = new InMemoryDataReaderDAO();
        DefaultDataStore store = new DefaultDataStore(new DatabusEventWriterRegistry(), new InMemoryTableDAO(), dataDao, dataDao,
                new NullSlowQueryLog(), new DiscardingExecutorService(), new InMemoryHistoryStore(),
                Optional.empty(), new InMemoryCompactionControlSource(), Conditions.alwaysFalse(),
                new DiscardingAuditWriter(), new InMemoryMapStore<>(), new MetricRegistry(), Clock.systemUTC(), new KafkaProducerService());

        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        store.createTable(TABLE, options, Collections.emptyMap(), newAudit("create table"));

        // First delta
        UUID uuid0 = TimeUUIDs.newUUID();
        store.updateAll(Collections.singleton(new Update(TABLE, KEY, uuid0, Deltas.fromString("{..,\"name\":\"Bob\"}"), newAudit("submit"),
                WriteConsistency.STRONG)), ImmutableSet.of("tag0"));

        // Compact the records
        store.compact(TABLE, KEY, Duration.ZERO, ReadConsistency.STRONG, WriteConsistency.STRONG);

        // Second delta, content and tags are unchanged.
        UUID uuid1 = TimeUUIDs.newUUID();
        store.updateAll(Collections.singleton(new Update(TABLE, KEY, uuid1, Deltas.fromString("{..,\"name\":\"Bob\"}"), newAudit("submit"),
                WriteConsistency.STRONG)), ImmutableSet.of("tag0"));

        DataProvider.AnnotatedContent result = getAnnotated(store, TABLE, KEY, ReadConsistency.STRONG);
        assertTrue(result.isChangeDeltaRedundant(uuid1));
    }

    @Test
    public void testJsonForLegacyCompactions() {
        // Test that legacy compactions work fine with new tagging
        // No lastTags attribute
        String legacyCompactionJson = "{\"count\":1," +
                "\"first\":\"9e278d70-1e09-11e6-a07b-26a39ee5ccb6\"," +
                "\"cutoff\":\"9e278d70-1e09-11e6-a07b-26a39ee5ccb6\"," +
                "\"cutoffSignature\":\"c7fb73f63ce47ec422bfccf2aaa37503\"," +
                "\"lastMutation\":\"9e278d70-1e09-11e6-a07b-26a39ee5ccb6\"," +
                "\"compactedDelta\":\"{\\\"name\\\":\\\"Bob\\\"}\"}";

        Compaction legacyCompaction = JsonHelper.fromJson(legacyCompactionJson, Compaction.class);
        assertTrue(legacyCompaction.getLastTags().isEmpty(), "Legacy compaction should have empty last tags");

        UUID uuid0 = TimeUUIDs.newUUID();
        final List<Map.Entry<DeltaClusteringKey, Compaction>> compactions = ImmutableList.of(
                Maps.immutableEntry(new DeltaClusteringKey(uuid0, 1), legacyCompaction));
        UUID uuid1 = TimeUUIDs.newUUID();
        final List<Map.Entry<DeltaClusteringKey, Change>> deltas = ImmutableList.of(
                Maps.immutableEntry(new DeltaClusteringKey(uuid0, 1), ChangeBuilder.just(uuid0, legacyCompaction)),
                Maps.immutableEntry(new DeltaClusteringKey(uuid1, 1), ChangeBuilder.just(uuid1, Deltas.fromString("{..,\"name\":\"Bob\"}")))); // uuid1 is redundant
        final List<Map.Entry<DeltaClusteringKey, Change>> deltas2 = ImmutableList.of(
                Maps.immutableEntry(new DeltaClusteringKey(uuid0, 1),ChangeBuilder.just(uuid0, legacyCompaction)),
                Maps.immutableEntry(new DeltaClusteringKey(uuid1, 1), ChangeBuilder.just(uuid1,
                        Deltas.fromString("{..,\"name\":\"Bob\", \"~tags\":[\"tag0\"]}"), ImmutableSet.of("tag0")))); // uuid1 is different

        Key key = mock(Key.class);
        Record record = mock(Record.class);
        when(record.getKey()).thenReturn(key);
        when(record.passOneIterator()).thenReturn(compactions.iterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas.iterator()).thenReturn(deltas2.iterator());

        long now = System.currentTimeMillis();
        MetricRegistry metricRegistry = new MetricRegistry();
        Counter archiveDeltaSize = metricRegistry.counter(MetricRegistry.name("bv.emodb.sor", "DistributedCompactor", "archivedDeltaSize"));
        Expanded expanded =
                new DistributedCompactor(archiveDeltaSize, true, metricRegistry)
                        .expand(record, now, now, now, MutableIntrinsics.create(key), false, mock(Supplier.class));

        assertTrue(expanded.getResolved().isChangeDeltaRedundant(uuid1), "Legacy compaction issue");

        expanded = new DistributedCompactor(archiveDeltaSize, true, metricRegistry)
                        .expand(record, now, now, now, MutableIntrinsics.create(key), false, mock(Supplier.class));
        assertFalse(expanded.getResolved().isChangeDeltaRedundant(uuid1), "Legacy compaction issue");

    }

    @Test
    public void testPartialCompactionWithNoRedundancy() throws Exception {
        InMemoryDataReaderDAO dataDao = new InMemoryDataReaderDAO();
        InMemoryTableDAO tableDao = new InMemoryTableDAO();

        DefaultDataStore store = new DefaultDataStore(new DatabusEventWriterRegistry(), tableDao, dataDao, dataDao,
                new NullSlowQueryLog(), new DiscardingExecutorService(), new InMemoryHistoryStore(),
                Optional.empty(), new InMemoryCompactionControlSource(), Conditions.alwaysFalse(),
                new DiscardingAuditWriter(), new InMemoryMapStore<>(), new MetricRegistry(), Clock.systemUTC(), new KafkaProducerService());

        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        store.createTable(TABLE, options, Collections.emptyMap(), newAudit("create table"));
        Table table = tableDao.get(TABLE);

        // Set the full consistency timestamp before the first delta
        dataDao.setFullConsistencyTimestamp(1408977300000L);

        // Create an update where there are no redundant deltas
        DeltaClusteringKey unique0 = new DeltaClusteringKey(TimeUUIDs.uuidForTimeMillis(1408977310000L), 1);
        DeltaClusteringKey unique1 = new DeltaClusteringKey(TimeUUIDs.uuidForTimeMillis(1408977320000L), 1);
        DeltaClusteringKey unique2 = new DeltaClusteringKey(TimeUUIDs.uuidForTimeMillis(1408977330000L), 1);
        DeltaClusteringKey unique3 = new DeltaClusteringKey(TimeUUIDs.uuidForTimeMillis(1408977340000L), 1);

        store.update(TABLE, KEY, unique0.getChangeId(), Deltas.fromString("{\"name\":\"Bob\"}"), newAudit("submit"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, unique1.getChangeId(), Deltas.fromString("{\"name\":\"Carol\"}"), newAudit("resubmit"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, unique2.getChangeId(), Deltas.fromString("{\"name\":\"Ted\"}"), newAudit("resubmit"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, unique3.getChangeId(), Deltas.fromString("{\"name\":\"Alice\"}"), newAudit("resubmit"), WriteConsistency.STRONG);

        // Set the full consistency timestamp such that no compaction will take place
        dataDao.setFullConsistencyTimestamp(1408977300000L);
        store.compact(TABLE, KEY, null, ReadConsistency.STRONG, WriteConsistency.STRONG);

        Record record = dataDao.read(new Key(table, KEY), ReadConsistency.STRONG);
        assertFalse(record.passOneIterator().hasNext());
        assertEquals(ImmutableList.of(unique0, unique1, unique2, unique3), toClusteringKeys(record.passTwoIterator()));

        // Set the full consistency timestamp so that only the first records are compacted
        dataDao.setFullConsistencyTimestamp(1408977325000L);
        store.compact(TABLE, KEY, null, ReadConsistency.STRONG, WriteConsistency.STRONG);

        record = dataDao.read(new Key(table, KEY), ReadConsistency.STRONG);
        Map.Entry<DeltaClusteringKey, Compaction> compactionEntry = Iterators.getOnlyElement(record.passOneIterator());
        Compaction compaction = compactionEntry.getValue();
        assertEquals(unique0.getChangeId(), compaction.getFirst());
        assertEquals(unique1.getChangeId(), compaction.getCutoff());
        assertEquals(unique1.getChangeId(), compaction.getLastMutation());
        // Deltas will not get deleted since compaction is still out of FCT. For this test, we don't need the deltas to be deleted.
        assertEquals(toClusteringKeys(record.passTwoIterator()), ImmutableList.of(unique0, unique1, unique2, unique3, compactionEntry.getKey()));

        // Repeat again such that all deltas are compacted
        dataDao.setFullConsistencyTimestamp(TimeUUIDs.getTimeMillis(TimeUUIDs.getNext(compactionEntry.getKey().getChangeId())) + 2000L);
        store.compact(TABLE, KEY, null, ReadConsistency.STRONG, WriteConsistency.STRONG);

        record = dataDao.read(new Key(table, KEY), ReadConsistency.STRONG);

        // We still keep the last compaction around since the new owning compaction will be out of FCT.
        int numOfCompactions = Iterators.advance(record.passOneIterator(), 10);
        assertEquals(numOfCompactions, 2, "Expect 2 compactions. The more recent is the effective one, " +
                "but we defer the owned compaction until later");
        DeltaClusteringKey oldCompactionKey = compactionEntry.getKey();
        record = dataDao.read(new Key(table, KEY), ReadConsistency.STRONG);

        Map.Entry<DeltaClusteringKey, Compaction> latestCompactionEntry = Iterators.getOnlyElement(
                Iterators.filter(record.passOneIterator(), input -> !input.getKey().equals(oldCompactionKey)));
        compaction = latestCompactionEntry.getValue();
        assertEquals(unique0.getChangeId(), compaction.getFirst());
        assertEquals(unique3.getChangeId(), compaction.getCutoff());
        assertEquals(unique3.getChangeId(), compaction.getLastMutation());
        assertEquals(toClusteringKeys(record.passTwoIterator()), ImmutableList.of(unique2, unique3, oldCompactionKey, latestCompactionEntry.getKey()),
                "Expecting unique2, and unique3 deltas");
    }

    @Test
    public void testPartialCompactionWithRedundancy() throws Exception {
        InMemoryDataReaderDAO dataDao = new InMemoryDataReaderDAO();
        InMemoryTableDAO tableDao = new InMemoryTableDAO();

        DefaultDataStore store = new DefaultDataStore(new DatabusEventWriterRegistry(), tableDao, dataDao, dataDao,
                new NullSlowQueryLog(), new DiscardingExecutorService(), new InMemoryHistoryStore(),
                Optional.empty(), new InMemoryCompactionControlSource(), Conditions.alwaysFalse(),
                new DiscardingAuditWriter(), new InMemoryMapStore<>(), new MetricRegistry(), Clock.systemUTC(), new KafkaProducerService());

        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        store.createTable(TABLE, options, Collections.emptyMap(), newAudit("create table"));
        Table table = tableDao.get(TABLE);

        // Set the full consistency timestamp before the first delta
        dataDao.setFullConsistencyTimestamp(1408977300000L);

        // Create an update where the last four updates are redundant
        UUID unique0 = TimeUUIDs.uuidForTimeMillis(1408977310000L);
        UUID unique1 = TimeUUIDs.uuidForTimeMillis(1408977320000L);
        UUID redund0 = TimeUUIDs.uuidForTimeMillis(1408977330000L);
        UUID redund1 = TimeUUIDs.uuidForTimeMillis(1408977340000L);
        UUID redund2 = TimeUUIDs.uuidForTimeMillis(1408977350000L);
        UUID redund3 = TimeUUIDs.uuidForTimeMillis(1408977360000L);

        store.update(TABLE, KEY, unique0, Deltas.fromString("{\"name\":\"Bob\"}"), newAudit("submit"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, unique1, Deltas.fromString("{\"name\":\"Ted\"}"), newAudit("resubmit"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, redund0, Deltas.fromString("{\"name\":\"Ted\"}"), newAudit("resubmit"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, redund1, Deltas.fromString("{\"name\":\"Ted\"}"), newAudit("resubmit"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, redund2, Deltas.fromString("{\"name\":\"Ted\"}"), newAudit("resubmit"), WriteConsistency.STRONG);
        store.update(TABLE, KEY, redund3, Deltas.fromString("{\"name\":\"Ted\"}"), newAudit("resubmit"), WriteConsistency.STRONG);

        // Set the full consistency timestamp such that no compaction will take place
        dataDao.setFullConsistencyTimestamp(1408977300000L);
        store.compact(TABLE, KEY, null, ReadConsistency.STRONG, WriteConsistency.STRONG);

        Record record = dataDao.read(new Key(table, KEY), ReadConsistency.STRONG);
        assertFalse(record.passOneIterator().hasNext());
        assertEquals(ImmutableList.of(unique0, unique1, redund0, redund1, redund2, redund3), toChangeIds(record.passTwoIterator()));

        // Set the full consistency timestamp so that only the first two redundant records are compacted
        dataDao.setFullConsistencyTimestamp(1408977345000L);
        store.compact(TABLE, KEY, null, ReadConsistency.STRONG, WriteConsistency.STRONG);

        record = dataDao.read(new Key(table, KEY), ReadConsistency.STRONG);
        Map.Entry<DeltaClusteringKey, Compaction> compactionEntry = Iterators.getOnlyElement(record.passOneIterator());
        Compaction compaction = compactionEntry.getValue();
        assertEquals(unique0, compaction.getFirst());
        assertEquals(redund1, compaction.getCutoff());
        assertEquals(unique1, compaction.getLastMutation());
        assertEquals(ImmutableList.of(unique0, unique1, redund0, redund1, redund2, redund3, compactionEntry.getKey().getChangeId()), toChangeIds(record.passTwoIterator()));

        assertRedundantDelta(store, TABLE, KEY, redund0);
        assertRedundantDelta(store, TABLE, KEY, redund1);
        assertRedundantDelta(store, TABLE, KEY, redund2);
        assertRedundantDelta(store, TABLE, KEY, redund3);

        // Repeat again such that all deltas are compacted
        dataDao.setFullConsistencyTimestamp(TimeUUIDs.getTimeMillis(TimeUUIDs.getNext(compactionEntry.getKey().getChangeId())) + 2000L);
        store.compact(TABLE, KEY, null, ReadConsistency.STRONG, WriteConsistency.STRONG);

        record = dataDao.read(new Key(table, KEY), ReadConsistency.STRONG);

        // We still keep the last compaction around since the new owning compaction will be out of FCT.
        int numOfCompactions = Iterators.advance(record.passOneIterator(), 10);
        assertEquals(numOfCompactions, 2, "Expect 2 compactions. The more recent is the effective one, " +
                "but we defer the owned compaction until later");
        UUID oldCompactionKey = compactionEntry.getKey().getChangeId();
        Map.Entry<DeltaClusteringKey, Compaction> latestCompactionEntry = Iterators.getOnlyElement(
                Iterators.filter(record.passOneIterator(), input -> !input.getKey().getChangeId().equals(oldCompactionKey)));
        compaction = latestCompactionEntry.getValue();
        assertEquals(unique0, compaction.getFirst());
        assertEquals(redund3, compaction.getCutoff());
        assertEquals(unique1, compaction.getLastMutation());
        assertEquals(ImmutableList.of(redund2, redund3, oldCompactionKey, latestCompactionEntry.getKey().getChangeId()), toChangeIds(record.passTwoIterator()));

        assertRedundantDelta(store, TABLE, KEY, redund0);
        assertRedundantDelta(store, TABLE, KEY, redund1);
        assertRedundantDelta(store, TABLE, KEY, redund2);
        assertRedundantDelta(store, TABLE, KEY, redund3);
    }

    private void assertChange(DefaultDataStore store, String table, String key, UUID changeId, Map<String, ?> expected)
            throws Exception {
        DataProvider.AnnotatedContent result = getAnnotated(store, table, key, ReadConsistency.STRONG);

        assertFalse(result.isChangeDeltaPending(changeId));
        assertFalse(result.isChangeDeltaRedundant(changeId));

        Map<String, Object> actual = result.getContent();

        // ignore the intrinsic _* keys such as ~id
        Map<String, Object> filtered =  excludeKeys(actual, Intrinsic.DATA_FIELDS);
        assertEquals(filtered, expected);
    }

    private void assertUnknownDelta(DefaultDataStore store, String table, String key, UUID changeId) {
        assertTrue(getAnnotated(store, table, key, ReadConsistency.STRONG).isChangeDeltaPending(changeId));
    }

    private void assertRedundantDelta(DefaultDataStore store, String table, String key, UUID changeId) {
        assertTrue(getAnnotated(store, table, key, ReadConsistency.STRONG).isChangeDeltaRedundant(changeId));
    }

    private DataProvider.AnnotatedContent getAnnotated(DataProvider dataProvider, String table, String key, ReadConsistency consistency) {
        return dataProvider.prepareGetAnnotated(consistency).add(table, key).execute().next();
    }

    private Audit newAudit(String comment) {
        return new AuditBuilder().
                setProgram("test").
                setLocalHost().
                setComment(comment).
                build();
    }

    private List<UUID> toChangeIds(Iterator<Map.Entry<DeltaClusteringKey, Change>> iterator) {
        return ImmutableList.copyOf(
                Iterators.transform(iterator, entry -> entry.getKey().getChangeId()));
    }

    private List<DeltaClusteringKey> toClusteringKeys(Iterator<Map.Entry<DeltaClusteringKey, Change>> iterator) {
        return ImmutableList.copyOf(
                Iterators.transform(iterator, entry -> entry.getKey()));
    }

    private List<UUID> toDeltaIds(Iterator<Map.Entry<UUID, Change>> iterator) {
        return ImmutableList.copyOf(
                Iterators.transform(Iterators.filter(iterator, input -> input.getValue().getCompaction() == null),
                        entry -> entry.getKey()));
    }

    private <K, V> Map<K, V> excludeKeys(Map<K, V> map, Set<String> keys) {
        return Maps.filterKeys(map, Predicates.not(Predicates.in(keys)));
    }
}
