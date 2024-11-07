package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.ChangeBuilder;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.db.test.DeltaClusteringKey;
import com.bazaarvoice.emodb.sor.db.test.InMemoryDataReaderDAO;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.kafka.KafkaProducerService;
import com.bazaarvoice.emodb.sor.test.SystemClock;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.table.db.Table;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class CompactorTest {

    /**
     * Simulate backward compatibility for compactions
     */
    @Test
    public void testLegacyCompaction() {
        // Old style compactions simulation
        final Key key = mock(Key.class);
        UUID t1 = TimeUUIDs.newUUID(); // First write
        UUID t2 = TimeUUIDs.newUUID(); // Second write
        UUID t3 = TimeUUIDs.newUUID(); // Third write
        UUID t5 = TimeUUIDs.newUUID(); // Legacy compaction compacts t1, t2, t3
        UUID t6 = TimeUUIDs.newUUID(); // Fourth write
        UUID t7 = TimeUUIDs.newUUID(); // New-style compaction

        // Wait 1 ms so that now is guaranteed to be after the last UUID created above
        SystemClock.tick();

        // Compaction and delta records after the second compaction
        Delta delta2 = Deltas.literal(ImmutableMap.of("key", "value"));
        Delta delta3 = Deltas.mapBuilder().put("key2", "change").build();
        // Old style compaction
        Compaction compaction2 = new Compaction(2, t1, t3, "abcdef0123456789", t3, t3);
        final List<Map.Entry<DeltaClusteringKey, Compaction>> compactions2 = ImmutableList.of(
                Maps.immutableEntry(new DeltaClusteringKey(t5 ,1), compaction2));
        final List<Map.Entry<DeltaClusteringKey,Change>> deltas2 = ImmutableList.of(
                Maps.immutableEntry(new DeltaClusteringKey(t3, 1), ChangeBuilder.just(t3, delta2)),
                Maps.immutableEntry(new DeltaClusteringKey(t5, 1), ChangeBuilder.just(t5, compaction2)),
                Maps.immutableEntry(new DeltaClusteringKey(t6, 1), ChangeBuilder.just(t6, delta3)));

        // This record will make it to LegacyCompactor, but will not "double-dip" in Cassandra as it has already found
        // compaction.
        Record record = mock(Record.class);
        when(record.getKey()).thenReturn(key);
        when(record.passOneIterator()).thenReturn(compactions2.iterator());
        when(record.passTwoIterator()).thenReturn(deltas2.iterator());

        //noinspection unchecked
        Supplier<Record> requeryFn = mock(Supplier.class);

        // Let's go to the new DistributedCompactor and make sure that the legacy compaction converts to DistributedCompaction.
        SystemClock.tick();
        long now = System.currentTimeMillis();
        MetricRegistry metricRegistry = new MetricRegistry();
        Counter archiveDeltaSize = metricRegistry.counter(MetricRegistry.name("bv.emodb.sor", "DistributedCompactor", "archivedDeltaSize"));
        Expanded expanded =
                new DistributedCompactor(archiveDeltaSize, true, metricRegistry)
                        .expand(record, now, now, now, MutableIntrinsics.create(key), false, requeryFn);

        // Methods that return iterators may not be called more than once.
        verify(record, times(1)).passOneIterator();
        verify(record, times(1)).passTwoIterator();
        // Verify we never require to requery fn
        verifyNoMoreInteractions(record, requeryFn);

        // Verify that expansion resolved to the expected results even with "old" compaction records
        Map<String, String> expectedContent = ImmutableMap.of("key", "value", "key2", "change");
        assertEquals(expanded.getResolved().getContent(), expectedContent);
        assertEquals(expanded.getResolved().getIntrinsics().getVersion(), 4);  // 4 writes: t1, t2, t3, t6
        assertEquals(expanded.getResolved().getIntrinsics().getFirstUpdateAtUuid(), t1);
        assertEquals(expanded.getResolved().getIntrinsics().getLastUpdateAtUuid(), t6);
        assertEquals(expanded.getNumDeletedDeltas(), 2);
        assertEquals(expanded.getNumPersistentDeltas(), 2);
        assertTrue(expanded.getPendingCompaction() != null);
        assertEquals(expanded.getPendingCompaction().getChangeId(), t6);
        assertEquals(expanded.getPendingCompaction().getDelta(), Deltas.literal(expectedContent));
        assertEquals(expanded.getPendingCompaction().getCompaction().getCount(), 3/*add 1 for cutoff delta*/+1);
        assertEquals(expanded.getPendingCompaction().getCompaction().getFirst(), t1);
        assertEquals(expanded.getPendingCompaction().getCompaction().getCutoff(), t6);

        // Verify that the pending compaction is a new-style compaction
        // This pending compaction should compact all the deltas available and replace them with compaction column
        assertNotNull(expanded.getPendingCompaction(), "Pending compaction does not exist");
        assertNotNull(expanded.getPendingCompaction().getCompaction(), "No compaction found in compaction");
        // The following test verifies that the pending compaction is a new style compaction.
        assertNotNull(expanded.getPendingCompaction().getCompaction().getCompactedDelta(), "Not a new version of compaction");

        Compaction newCompaction = expanded.getPendingCompaction().getCompaction();
        final List<Map.Entry<DeltaClusteringKey, Compaction>> compactions3 = ImmutableList.of(
                Maps.immutableEntry(new DeltaClusteringKey(t7, 1), newCompaction));
        final List<Map.Entry<DeltaClusteringKey,Change>> deltas3 = ImmutableList.of(
                Maps.immutableEntry(new DeltaClusteringKey(t7, 1), ChangeBuilder.just(t7, newCompaction)));

        Record record3 = mock(Record.class);
        when(record3.getKey()).thenReturn(key);
        when(record3.passOneIterator()).thenReturn(compactions3.iterator());
        when(record3.passTwoIterator()).thenReturn(deltas3.iterator());

        // This time it should not delegate to DefaultCompactor since the resulting compaction was a new compaction
        Expanded expanded3 =
                new DistributedCompactor(archiveDeltaSize, true, metricRegistry)
                        .expand(record3, now, now, now, MutableIntrinsics.create(key), false, requeryFn);

        // Verify we never require to requery fn
        verify(record3, times(1)).passOneIterator();
        verify(record3, times(1)).passTwoIterator();
        verifyNoMoreInteractions(record3, requeryFn);

        // Verify content with new compaction this time
        assertEquals(expanded3.getResolved().getContent(), expectedContent);
        assertEquals(expanded3.getResolved().getIntrinsics().getVersion(), 4);  // 4 writes: t1, t2, t3, t6
        assertEquals(expanded3.getResolved().getIntrinsics().getFirstUpdateAtUuid(), t1);
        assertEquals(expanded3.getResolved().getIntrinsics().getLastUpdateAtUuid(), t6);
        assertEquals(expanded3.getNumDeletedDeltas(), 4);
        assertEquals(expanded3.getNumPersistentDeltas(), 0);
    }

    /** Simulate compaction occurring between Record.passOneIterator and Record.passTwoIterator.
     *  Note that this test is for legacy Compactor that restarts itself in case of race conditions between two
     *  compactor threads.
     */
    @Test
    public void testRestart()
            throws Exception {
        final Key key = mock(Key.class);
        UUID t1 = TimeUUIDs.newUUID(); // First write
        UUID t2 = TimeUUIDs.newUUID(); // Second write
        UUID t3 = TimeUUIDs.newUUID(); // Third write
        UUID t4 = TimeUUIDs.newUUID(); // First compaction compacts t1 and t2
        UUID t5 = TimeUUIDs.newUUID(); // Second compaction compacts t1, t2, t3
        UUID t6 = TimeUUIDs.newUUID(); // Fourth write

        // Wait 1 ms so that now is guaranteed to be after the last UUID created above
        SystemClock.tick();

        // Compaction records after the first compaction
        Compaction compaction1 = new Compaction(2, t1, t2, "0123456789abcdef", t2, t2);
        final List<Map.Entry<DeltaClusteringKey, Compaction>> compactions1 = ImmutableList.of(
                Maps.immutableEntry(new DeltaClusteringKey(t4, 1), compaction1));

        // Compaction and delta records after the second compaction
        Delta delta2 = Deltas.literal(ImmutableMap.of("key", "value"));
        Delta delta3 = Deltas.mapBuilder().put("key2", "change").build();
        Compaction compaction2 = new Compaction(2, t1, t3, "abcdef0123456789", t3, t3);
        final List<Map.Entry<DeltaClusteringKey, Compaction>> compactions2 = ImmutableList.of(
                Maps.immutableEntry(new DeltaClusteringKey(t5, 1), compaction2));
        final List<Map.Entry<DeltaClusteringKey, Change>> deltas2 = ImmutableList.of(
                Maps.immutableEntry(new DeltaClusteringKey(t3, 1), ChangeBuilder.just(t3, delta2)),
                Maps.immutableEntry(new DeltaClusteringKey(t5, 1), ChangeBuilder.just(t5, compaction2)),
                Maps.immutableEntry(new DeltaClusteringKey(t6, 1), ChangeBuilder.just(t6, delta3)));

        // First try will delegate to legacy compactor and fail because compaction record 1 is not present in the 2nd sequence of deltas
        Record record1 = mock(Record.class);
        when(record1.getKey()).thenReturn(key);
        when(record1.passOneIterator()).thenReturn(compactions1.iterator());
        when(record1.passTwoIterator()).thenReturn(deltas2.iterator());

        // Second try should succeed - while also delegating back to legacy compactor.
        Record record2 = mock(Record.class);
        when(record2.getKey()).thenReturn(key);
        when(record2.passOneIterator()).thenReturn(compactions2.iterator());
        when(record2.passTwoIterator()).thenReturn(deltas2.iterator());

        //noinspection unchecked
        Supplier<Record> requeryFn = mock(Supplier.class);
        when(requeryFn.get()).thenReturn(record2);

        long now = System.currentTimeMillis();
        MetricRegistry metricRegistry = new MetricRegistry();
        Counter archiveDeltaSize = metricRegistry.counter(MetricRegistry.name("bv.emodb.sor", "DistributedCompactor", "archivedDeltaSize"));
        Expanded expanded =
                new DistributedCompactor(archiveDeltaSize, true, metricRegistry)
                .expand(record1, now, now, now, MutableIntrinsics.create(key), false, requeryFn);

        // Methods that return iterators may not be called more than once.
        verify(record1, times(1)).passOneIterator();
        verify(record1, times(1)).passTwoIterator();
        verify(requeryFn).get();
        verify(record2, times(1)).passOneIterator();
        verify(record2, times(1)).passTwoIterator();
        verifyNoMoreInteractions(record1, record2, requeryFn);

        // Verify that expansion resolved to the expected results.
        Map<String, String> expectedContent = ImmutableMap.of("key", "value", "key2", "change");
        assertEquals(expanded.getResolved().getContent(), expectedContent);
        assertEquals(expanded.getResolved().getIntrinsics().getVersion(), 4);  // 4 writes: t1, t2, t3, t6
        assertEquals(expanded.getResolved().getIntrinsics().getFirstUpdateAtUuid(), t1);
        assertEquals(expanded.getResolved().getIntrinsics().getLastUpdateAtUuid(), t6);
        assertEquals(expanded.getNumDeletedDeltas(), 2);
        assertEquals(expanded.getNumPersistentDeltas(), 2);
        assertTrue(expanded.getPendingCompaction() != null);
        assertEquals(expanded.getPendingCompaction().getChangeId(), t6);
        assertEquals(expanded.getPendingCompaction().getDelta(), Deltas.literal(expectedContent));
        assertEquals(expanded.getPendingCompaction().getCompaction().getCount(), 3/*add 1 for cutoff delta*/+1);
        assertEquals(expanded.getPendingCompaction().getCompaction().getFirst(), t1);
        assertEquals(expanded.getPendingCompaction().getCompaction().getCutoff(), t6);
    }

    @Test
    public void testDisableDeltaHistory() {
        final Key key = mock(Key.class);
        UUID t1 = TimeUUIDs.newUUID(); // First write
        UUID t2 = TimeUUIDs.newUUID(); // Second write
        UUID t3 = TimeUUIDs.newUUID(); // Third write

        // Wait 1 ms so that now is guaranteed to be after the last UUID created above
        SystemClock.tick();

        Delta delta2 = Deltas.literal(ImmutableMap.of("key", "value"));
        Delta delta3 = Deltas.mapBuilder().put("key2", "change").build();
        final List<Map.Entry<DeltaClusteringKey, Compaction>> compactions = Lists.newArrayList();
        final List<Map.Entry<DeltaClusteringKey, Change>> deltas2 = ImmutableList.of(
                Maps.immutableEntry(new DeltaClusteringKey(t1, 1), ChangeBuilder.just(t1, delta2)),
                Maps.immutableEntry(new DeltaClusteringKey(t2, 1), ChangeBuilder.just(t2, delta2)),
                Maps.immutableEntry(new DeltaClusteringKey(t3, 1), ChangeBuilder.just(t3, delta3)));

        Record record = mock(Record.class);
        when(record.getKey()).thenReturn(key);
        when(record.passOneIterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas2.iterator());

        //noinspection unchecked
        Supplier<Record> requeryFn = mock(Supplier.class);
        when(requeryFn.get()).thenReturn(record);

        long now = System.currentTimeMillis();
        MetricRegistry metricRegistry = new MetricRegistry();
        Counter archiveDeltaSize = metricRegistry.counter(MetricRegistry.name("bv.emodb.sor", "DefaultCompactor", "archivedDeltaSize"));
        boolean isDeltaHistoryEnabled = false;
        Expanded expanded = new DistributedCompactor(archiveDeltaSize, isDeltaHistoryEnabled, metricRegistry)
                .expand(record, now, now, now, MutableIntrinsics.create(key), false, requeryFn);

        // Verify that expansion produces a compaction with no delta archives
        Map<String, String> expectedContent = ImmutableMap.of("key", "value", "key2", "change");
        assertEquals(expanded.getResolved().getContent(), expectedContent);
        assertTrue(expanded.getPendingCompaction() != null);
        assertTrue(expanded.getPendingCompaction().getDeltasToArchive().isEmpty(), "Delta history is disabled");

        // Now verify we do get delta history if enabled
        Record record2 = mock(Record.class);
        when(record2.getKey()).thenReturn(key);
        when(record2.passOneIterator()).thenReturn(compactions.iterator());
        when(record2.passTwoIterator()).thenReturn(deltas2.iterator());
        isDeltaHistoryEnabled = true;
        expanded = new DistributedCompactor(archiveDeltaSize, isDeltaHistoryEnabled, metricRegistry)
                .expand(record2, now, now, now, MutableIntrinsics.create(key), false, requeryFn);
        expectedContent = ImmutableMap.of("key", "value", "key2", "change");
        assertEquals(expanded.getResolved().getContent(), expectedContent);
        assertTrue(expanded.getPendingCompaction() != null);
        assertEquals(expanded.getPendingCompaction().getDeltasToArchive().size(), 3, "Archive 3 deltas");
    }

    /**
     * Proof that System of Record does not need tombstones, i.e., gc_grace_seconds can be set to 0
     * Basically, the effect of not keeping tombstones in Cassandra nodes upon compaction is that some
     * nodes may "resurrect" the deletes. The only deletes that happen in System of Record is when compaction
     * takes place, and old deltas get deleted. Emo deletes/compacts only the deltas whose change Id is before
     * Full Consistency Timestamp (FCT).
     *
     * This test proves that even if deleted deltas resurrect themselves, Emodb would simply delete them again in
     * a subsequent compaction without any impact to data.
     *
     * Update: Although technically correct, there is an implementation level concern. Cassandra sets the hints
     * expiration to gc_grace_seconds. You can set hints expiration to less than gc_grace, but not more than that.
     * If these hints expire (or are never generated when gc_grace_seconds is set to 0), our FCT algorithm will think
     * we are fully consistent, when we may not be. Obviously, this will be a problem for several reasons but primarily
     * delta compactions may result in data loss.
     *
     * So, we should set our gc_grace_seconds not due to concern for tombstones, but out of hints expiration concern.
     * Cassandra's default recommendation is gc_grace_seconds to 10 days, and a weekly repair.
     * References:
     * https://issues.apache.org/jira/browse/CASSANDRA-5988
     * https://issues.apache.org/jira/browse/CASSANDRA-5314
     *
     * Ideally, Cassandra should allow the users to make the call on hints TTL independently from gc_grace_seconds.
     */
    @Test
    public void testTombstonesDoNotMatter() {
        final Key key = mock(Key.class);
        UUID t1 = TimeUUIDs.newUUID(); // First write
        UUID t2 = TimeUUIDs.newUUID(); // Second write
        UUID t3 = TimeUUIDs.newUUID(); // Third write

        // Wait 1 ms so that now is guaranteed to be after the last UUID created above
        // Record full consistency timestamp right after t3 for later use
        long fctRightAfterT3 = TimeUUIDs.getTimeMillis(TimeUUIDs.getNext(t3));
        SystemClock.tick();

        Delta delta2 = Deltas.literal(ImmutableMap.of("key", "value"));
        Delta delta3 = Deltas.mapBuilder().put("key2", "change").build();
        final List<Map.Entry<DeltaClusteringKey, Compaction>> compactions = Lists.newArrayList();
        Map.Entry<DeltaClusteringKey, Change> firstDelta = Maps.immutableEntry(new DeltaClusteringKey(t1, 1), ChangeBuilder.just(t1, delta2));
        final List<Map.Entry<DeltaClusteringKey, Change>> deltas2 = Lists.newArrayList(firstDelta,
                Maps.immutableEntry(new DeltaClusteringKey(t2, 1), ChangeBuilder.just(t2, delta2)),
                Maps.immutableEntry(new DeltaClusteringKey(t3, 1), ChangeBuilder.just(t3, delta3)));

        Record record = mock(Record.class);
        when(record.getKey()).thenReturn(key);
        when(record.passOneIterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas2.iterator());

        //noinspection unchecked
        Supplier<Record> requeryFn = mock(Supplier.class);

        long now = System.currentTimeMillis();
        MetricRegistry metricRegistry = new MetricRegistry();
        Counter archiveDeltaSize = metricRegistry.counter(MetricRegistry.name("bv.emodb.sor", "DefaultCompactor", "archivedDeltaSize"));
        Compactor compactor = new DistributedCompactor(archiveDeltaSize, false, metricRegistry);
        Expanded expanded = compactor.expand(record, now, now, Long.MIN_VALUE, MutableIntrinsics.create(key), false, requeryFn);

        // Verify that expansion produces a compaction, but does *not* delete the compaction owned deltas. That will happen in the next read
        Map<String, String> expectedContent = ImmutableMap.of("key", "value", "key2", "change");
        assertEquals(expanded.getResolved().getContent(), expectedContent);
        assertTrue(expanded.getPendingCompaction() != null);
        // Do not delete deltas just yet
        assertTrue(expanded.getPendingCompaction().getKeysToDelete().isEmpty());
        // Add the compactions to the compaction list
        compactions.add(Maps.immutableEntry(new DeltaClusteringKey(expanded.getPendingCompaction().getChangeId(), 1), expanded.getPendingCompaction().getCompaction()));
        // Resetting now
        SystemClock.tick();
        now = System.currentTimeMillis();

        when(record.passOneIterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas2.iterator());

        expanded = compactor.expand(record, now, now, Long.MIN_VALUE, MutableIntrinsics.create(key), false, requeryFn);
        // Verify that our deltas are going to be deleted now
        assertTrue(expanded.getPendingCompaction().getKeysToDelete().size() == 3, "All 3 deltas should be up for deletion");
        assertTrue(ImmutableSet.copyOf(expanded.getPendingCompaction().getKeysToDelete()).equals(ImmutableSet.of(new DeltaClusteringKey(t1, 1), new DeltaClusteringKey(t2, 1), new DeltaClusteringKey(t3, 1))));

        // Finally, let's assume only one delta really got deleted, and two of these deltas
        // "resurrected" themselves due to no tombstones in Cassandra
        deltas2.remove(firstDelta); // only _really_ deleting the first delta

        // Try again, and make sure that this time we delete the two resurrected deltas
        when(record.passOneIterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas2.iterator());

        expanded = compactor.expand(record, now, now, Long.MIN_VALUE, MutableIntrinsics.create(key), false, requeryFn);
        assertTrue(expanded.getPendingCompaction().getKeysToDelete().size() == 2, "The 2 'resurrected' deltas are simply deleted again");
        assertTrue(ImmutableSet.copyOf(expanded.getPendingCompaction().getKeysToDelete()).equals(ImmutableSet.of(new DeltaClusteringKey(t2, 1), new DeltaClusteringKey(t3, 1))));
        expectedContent = ImmutableMap.of("key", "value", "key2", "change");
        assertEquals(expanded.getResolved().getContent(), expectedContent);

        // Verify that a resurrected compaction is OK

        // Remember the only time we start a new compaction is when there is no compaction, or the
        // existing compaction is before the full consistency timestamp (FCT), i.e., it is fully consistent on all nodes.
        // To be sure, the following test asserts the above.
        // Artificially set the FCT after t3, but before the compaction
        when(record.passOneIterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas2.iterator());
        expanded = compactor.expand(record, fctRightAfterT3, fctRightAfterT3, Long.MIN_VALUE, MutableIntrinsics.create(key), false, requeryFn);
        // Even though there are deltas before the FCT, we do not compact since we have an outstanding compaction.
        assertTrue(expanded.getPendingCompaction() == null);

        SystemClock.tick();
        now = System.currentTimeMillis();
        // Add a new delta so we can discard the old compaction, and then resurrect compaction again for our test
        UUID t4 = TimeUUIDs.newUUID();
        Delta delta4 = Deltas.mapBuilder().put("key4", "change4").build();
        deltas2.add(Maps.immutableEntry(new DeltaClusteringKey(t4, 1), ChangeBuilder.just(t4, delta4)));
        when(record.passOneIterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas2.iterator());

        expanded = compactor.expand(record, now, now, Long.MIN_VALUE, MutableIntrinsics.create(key), false, requeryFn);
        // The above should create a new compaction
        assertTrue(expanded.getPendingCompaction().getCompaction() != null);
        DeltaClusteringKey toBeResurrectedCompaction = compactions.get(0).getKey();
        // Make sure the old compaction is getting deleted
        assertTrue(expanded.getPendingCompaction().getKeysToDelete().contains(toBeResurrectedCompaction));
        expectedContent = ImmutableMap.of("key", "value", "key2", "change", "key4", "change4");
        assertEquals(expanded.getResolved().getContent(), expectedContent);

        // Add the newest compaction to our list of compaction, but do not delete the old one simulating resurrection
        compactions.add(Maps.immutableEntry(new DeltaClusteringKey(expanded.getPendingCompaction().getChangeId(), 1), expanded.getPendingCompaction().getCompaction()));
        // Let's fetch the record again, and see if the existing old compaction affect anything
        when(record.passOneIterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas2.iterator());
        expanded = compactor.expand(record, now, now, Long.MIN_VALUE, MutableIntrinsics.create(key), false, requeryFn);
        // The resurrected compaction should be deleted again
        assertTrue(expanded.getPendingCompaction().getKeysToDelete().contains(toBeResurrectedCompaction));
        // No changes to the content
        assertEquals(expanded.getResolved().getContent(), expectedContent);
    }

    @Test
    public void testCorruption()
            throws InterruptedException {
        final String tableName = "test:corruption";
        final String placement = "app_global:default";
        final String key = "corruptedKey";
        // Latch used to coordinate compactin events
        final CountDownLatch compactionDeletesLatch = new CountDownLatch(1);
        final CountDownLatch addCompactionLatch = new CountDownLatch(1);

        // Boolean used to control whether the data DAO should pause after deletion of records or not
        final AtomicBoolean holdRecord = new AtomicBoolean(true);
        InMemoryDataReaderDAO dataDAO = new InMemoryDataReaderDAO() {
            @Override
            public void compact(Table table, String key, UUID compactionKey, Compaction compaction,
                                UUID changeId, Delta delta, Collection<DeltaClusteringKey> changesToDelete, List<History> historyList, WriteConsistency consistency) {
                requireNonNull(table, "table");
                requireNonNull(key, "key");
                requireNonNull(compactionKey, "compactionKey");
                requireNonNull(compaction, "compaction");
                requireNonNull(changeId, "changeId");
                requireNonNull(delta, "delta");
                requireNonNull(changesToDelete, "changesToDelete");

                Map<UUID, Change> changes = super.safePut(super._contentChanges, table.getName(), key);

//                // delete the old deltas & compaction records
//                super.deleteDeltas(changesToDelete, changes);

                // add the compaction record and update the last content of the last delta
                super.addCompaction(compactionKey, compaction, changeId, delta, changes);

                // This countdown latch is here for testing race condition scenarios
                if (holdRecord.get()) {
                    compactionDeletesLatch.countDown();
                    // Wait till the other compaction is done
                    try {
                        addCompactionLatch.await();
                    } catch (InterruptedException e) {
                        throw Throwables.propagate(e);
                    }
                }

//                // add the compaction record and update the last content of the last delta
//                super.addCompaction(compactionKey, compaction, changeId, delta, changes);

                // delete the old deltas & compaction records
                super.deleteDeltas(changesToDelete, changes);

                // Add delta histories
                if (historyList != null && !historyList.isEmpty()) {
                    super._historyStore.putDeltaHistory(table.getName(), key, historyList);
                }
            }
        };

        final DataStore dataStore = new InMemoryDataStore(dataDAO, new MetricRegistry(), mock(KafkaProducerService.class));

        // Create a table for our test
        dataStore.createTable(tableName,
                new TableOptionsBuilder().setPlacement(placement).build(),
                ImmutableMap.<String, Object>of(),
                new AuditBuilder().setComment("Corrupted Compaction scenario").build());

        createDelta(dataStore, tableName, key, "count", 0); // Delta 0
        createDelta(dataStore, tableName, key, "count", 1); // Delta 1
        createDelta(dataStore, tableName, key, "count", 2); // Delta 2
        createDelta(dataStore, tableName, key, "count1", 3); // Delta 3

        // Async compaction will try to compact d0, d1, d2 into d3.. but ends up deleting d0 d1 d2, before writing compaction
        Thread concurrentCompactionThread = new Thread(new Runnable() {
            @Override
            public void run() {
                dataStore.compact(tableName, key, Duration.ZERO, ReadConsistency.STRONG, WriteConsistency.STRONG);
            }
        });
        concurrentCompactionThread.start();

        // In the mean time, another compaction comes around and will try to compact d3, d4 into d5, and deletes d3, d4
        // Another node starts concurrent compaction
        compactionDeletesLatch.await();
        createDelta(dataStore, tableName, key, "count2", 4); // Delta 4
        createDelta(dataStore, tableName, key, "count2", 5); // Delta 5


        holdRecord.set(false);
        dataStore.compact(tableName, key, Duration.ZERO, ReadConsistency.STRONG, WriteConsistency.STRONG);
        addCompactionLatch.countDown();

        // Now un-pause the previous compaction
        compactionDeletesLatch.countDown();

        // Allow the async compaction to finish to avoid ConcurrentModificationException
        concurrentCompactionThread.join();

        // Verify that you have corrupted the document
        Map<String, Object> map = dataStore.get(tableName, key);
        assertEquals(Integer.parseInt(map.get("~version").toString()), 6, "Version is not 6. This record is corrupted.");
    }

    /**
     * This unit test specifically checks for the condition detailed in EMO-5335.  Please refer to that
     * ticket in JIRA for a full discussion of the defect.
     * @throws Exception
     */
    @Test
    public void testConcurrentCompactionWithLazilyLoadedChanges() throws Exception {
        // Setup some constants used throughout this test
        final String tableName = "test:compactionloss";
        final String placement = "app_global:default";
        final String key = "thekey";
        final long now = System.currentTimeMillis();

        // Boolean used to control whether the data DAO should block after reading a record before returning
        final AtomicBoolean holdRecord = new AtomicBoolean(false);
        // Latch used to coordinate events after the record has been read
        final CountDownLatch recordRead = new CountDownLatch(1);
        // Latch used to coordinate returning the read record
        final CountDownLatch returnRecord = new CountDownLatch(1);

        InMemoryDataReaderDAO dataDAO = new InMemoryDataReaderDAO() {
            @Override
            public Record read(Key key, ReadConsistency ignored) {
                // Read the record as usual
                Record record = super.read(key, ignored);
                if (holdRecord.get()) {
                    // Signal that the record has been read
                    recordRead.countDown();
                    // Wait until the main thread signals that execution should resume
                    try {
                        returnRecord.await();
                    } catch (InterruptedException e) {
                        throw Throwables.propagate(e);
                    }
                }
                return record;
            }
        };

        // Configure the data DAO to read 10 columns initially, causing other column reads to be read lazily
        dataDAO.setColumnBatchSize(10);

        final DataStore dataStore = new InMemoryDataStore(dataDAO, new MetricRegistry(), mock(KafkaProducerService.class));

        // Create a table for our test
        dataStore.createTable(tableName,
                new TableOptionsBuilder().setPlacement(placement).build(),
                ImmutableMap.<String, Object>of(),
                new AuditBuilder().setComment("Creating compaction loss test table").build());

        // Set the current full consistency timestamp in dataDAO to one day ago
        dataDAO.setFullConsistencyTimestamp(now - TimeUnit.DAYS.toMillis(1));

        // Later we'll update the full consistency timestamp to one minute in the past; calculate it now
        long fullConsistencyTs = now - TimeUnit.MINUTES.toMillis(1);

        // Create an initial delta for this row one hour in the past
        long timestamp = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1);
        createInitialValue(dataStore, tableName, key, timestamp);

        // Create 8 additional records all before the full consistency time
        for (int i=1; i <= 8; i++) {
            timestamp += TimeUnit.MINUTES.toMillis(1);
            createDelta(dataStore, tableName, key, i, timestamp);
        }

        // Create 1 more record which is after the full consistency timestamp
        createDelta(dataStore, tableName, key, 9, fullConsistencyTs - TimeUnit.SECONDS.toMillis(1));
        // There are now 10 deltas, which is the initial read column limit.  Create one more delta afterward
        createDelta(dataStore, tableName, key, 10, fullConsistencyTs + TimeUnit.SECONDS.toMillis(1));

        // Set the full consistency timestamp to the calculated value
        dataDAO.setFullConsistencyTimestamp(fullConsistencyTs);

        // Create a thread which will compact the record but block partway through, simulating a context switch
        holdRecord.set(true);   // Causes the record read to block
        Thread concurrentCompactionThread = new Thread(new Runnable() {
            @Override
            public void run() {
                dataStore.compact(tableName, key, null, ReadConsistency.STRONG, WriteConsistency.STRONG);
            }
        });
        concurrentCompactionThread.start();

        // Wait until the record has been read in the other thread but has not been resolved
        recordRead.await();

        // Perform a compaction concurrent to the other thread
        holdRecord.set(false);  // Causes the record to read normally without blocking
        dataStore.compact(tableName, key, null, ReadConsistency.STRONG, WriteConsistency.STRONG);

        // Release the other thread, allowing it to complete resolution and compaction
        returnRecord.countDown();

        // Wait for the other thread to finish compaction
        concurrentCompactionThread.join();

        // Verify the final object is as expected
        Map<String, Object> map = dataStore.get(tableName, key);
        assertEquals(map.get("constant"), "immutable-value");
        assertEquals(map.get("count"), 10);

        // Verify in the timeline that compaction did occur
        Iterator<Change> timeline = dataStore.getTimeline(tableName, key, true, false, null, null, false, 10000, ReadConsistency.STRONG);
        Iterator<Change> compactions = Iterators.filter(timeline, new Predicate<Change>() {
            @Override
            public boolean apply(Change change) {
                return change.getCompaction() != null;
            }
        });
        assertEquals(Iterators.size(compactions), 1);
    }

    private void createInitialValue(DataStore dataStore, String tableName, String key, long timestamp) {
        dataStore.update(tableName, key, com.bazaarvoice.emodb.common.uuid.TimeUUIDs.uuidForTimeMillis(timestamp),
                Deltas.mapBuilder()
                        .put("constant", "immutable-value")
                        .put("count", 0)
                        .build(),
                new AuditBuilder().setComment("initial value").build());
    }

    private void createDelta(DataStore dataStore, String tableName, String key, String attribute, int newCount) {
        dataStore.update(tableName, key, com.bazaarvoice.emodb.common.uuid.TimeUUIDs.uuidForTimeMillis(System.currentTimeMillis()),
                Deltas.mapBuilder()
                        .update(attribute, Deltas.literal(newCount))
                        .build(),
                new AuditBuilder().setComment("Updating count to " + newCount).build());
        SystemClock.tick();
    }
    private void createDelta(DataStore dataStore, String tableName, String key, int newCount, long timestamp) {
        createDelta(dataStore, tableName, key, "count", newCount, timestamp);
    }
    private void createDelta(DataStore dataStore, String tableName, String key, String attribute, int newCount, long timestamp) {
        dataStore.update(tableName, key, com.bazaarvoice.emodb.common.uuid.TimeUUIDs.uuidForTimeMillis(timestamp),
                Deltas.mapBuilder()
                        .updateIfExists(attribute, Deltas.literal(newCount))
                        .build(),
                new AuditBuilder().setComment("Updating count to " + newCount).build());
        SystemClock.tick();
    }
}