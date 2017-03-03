package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.ChangeBuilder;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class CompactionControlTest {

    @Test
    public void allDeltasBeforeCompactionControlTimestampShouldNotBeDeleted() {

        long nowInTimeMillis = System.currentTimeMillis();

        UUID t1 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis);
        UUID t2 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 10 * 1000);
        UUID t3 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 20 * 1000);

        // note: compaction control timestamp is after all the deltas.
        long compactionControlTimestamp = nowInTimeMillis + 30 * 1000;

        long fullConsistencyTimestamp = nowInTimeMillis + 40 * 1000;

        Delta delta1 = Deltas.literal(ImmutableMap.of("key1", "value1"));
        Delta delta2 = Deltas.literal(ImmutableMap.of("key2", "value2"));
        Delta delta3 = Deltas.mapBuilder().put("key3", "change").build();

        final List<Map.Entry<UUID, Change>> deltas = ImmutableList.of(
                Maps.immutableEntry(t1, ChangeBuilder.just(t1, delta1)),
                Maps.immutableEntry(t2, ChangeBuilder.just(t2, delta2)),
                Maps.immutableEntry(t3, ChangeBuilder.just(t3, delta3)));

        Record record = mock(Record.class);
        final Key key = mock(Key.class);
        when(record.getKey()).thenReturn(key);
        final List<Map.Entry<UUID, Compaction>> compactions = Lists.newArrayList();
        when(record.passOneIterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas.iterator());

        //noinspection unchecked
        Supplier<Record> requeryFn = mock(Supplier.class);
        when(requeryFn.get()).thenReturn(record);

        MetricRegistry metricRegistry = new MetricRegistry();
        Counter archiveDeltaSize = metricRegistry.counter(MetricRegistry.name("bv.emodb.sor", "DefaultCompactor", "archivedDeltaSize"));
        Compactor compactor = new DistributedCompactor(archiveDeltaSize, false, metricRegistry);
        Expanded expanded = compactor.expand(record, fullConsistencyTimestamp, fullConsistencyTimestamp, compactionControlTimestamp, MutableIntrinsics.create(key), false, requeryFn);

        assertTrue(expanded.getPendingCompaction() != null);
        // Do not delete deltas just yet; deltas are going to be deleted now in the next read
        assertTrue(expanded.getPendingCompaction().getKeysToDelete().isEmpty());
        // Add the compactions to the compaction list
        UUID compactionTimestamp = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 35 * 1000); // altering this instead of "expanded.getPendingCompaction().getChangeId()"
        compactions.add(Maps.immutableEntry(compactionTimestamp, expanded.getPendingCompaction().getCompaction()));
        when(record.passOneIterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas.iterator());

        expanded = compactor.expand(record, fullConsistencyTimestamp, fullConsistencyTimestamp, compactionControlTimestamp, MutableIntrinsics.create(key), false, requeryFn);

        // In the DistributedCompactor, at first 3 keys will be selected for deletion, and later all 3 keys get filtered out from the deletionList based on the compactionControlTimestamp.
        // Verify that there are 0 keys for deletion. If there are no keys to be deleted, there will be no pending compaction.
        assertNull(expanded.getPendingCompaction());
    }

    @Test
    public void allDeltasAfterCompactionControlTimestampShouldBeDeleted() {

        long nowInTimeMillis = System.currentTimeMillis();

        // note: compaction control timestamp is before all the deltas.
        long compactionControlTimestamp = nowInTimeMillis;

        UUID t1 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 10 * 1000);
        UUID t2 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 20 * 1000);
        UUID t3 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 30 * 1000);

        long fullConsistencyTimestamp = nowInTimeMillis + 40 * 1000;

        Delta delta1 = Deltas.literal(ImmutableMap.of("key1", "value1"));
        Delta delta2 = Deltas.literal(ImmutableMap.of("key2", "value2"));
        Delta delta3 = Deltas.mapBuilder().put("key3", "change").build();

        final List<Map.Entry<UUID, Change>> deltas = ImmutableList.of(
                Maps.immutableEntry(t1, ChangeBuilder.just(t1, delta1)),
                Maps.immutableEntry(t2, ChangeBuilder.just(t2, delta2)),
                Maps.immutableEntry(t3, ChangeBuilder.just(t3, delta3)));

        Record record = mock(Record.class);
        final Key key = mock(Key.class);
        when(record.getKey()).thenReturn(key);
        final List<Map.Entry<UUID, Compaction>> compactions = Lists.newArrayList();
        when(record.passOneIterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas.iterator());

        //noinspection unchecked
        Supplier<Record> requeryFn = mock(Supplier.class);
        when(requeryFn.get()).thenReturn(record);

        MetricRegistry metricRegistry = new MetricRegistry();
        Counter archiveDeltaSize = metricRegistry.counter(MetricRegistry.name("bv.emodb.sor", "DefaultCompactor", "archivedDeltaSize"));
        Compactor compactor = new DistributedCompactor(archiveDeltaSize, false, metricRegistry);
        Expanded expanded = compactor.expand(record, fullConsistencyTimestamp, fullConsistencyTimestamp, compactionControlTimestamp, MutableIntrinsics.create(key), false, requeryFn);

        assertTrue(expanded.getPendingCompaction() != null);
        // Do not delete deltas just yet; deltas are going to be deleted now in the next read
        assertTrue(expanded.getPendingCompaction().getKeysToDelete().isEmpty());
        // Add the compactions to the compaction list
        UUID compactionTimestamp = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 35 * 1000); // altering this instead of "expanded.getPendingCompaction().getChangeId()"
        compactions.add(Maps.immutableEntry(compactionTimestamp, expanded.getPendingCompaction().getCompaction()));
        when(record.passOneIterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas.iterator());

        expanded = compactor.expand(record, fullConsistencyTimestamp, fullConsistencyTimestamp, compactionControlTimestamp, MutableIntrinsics.create(key), false, requeryFn);

        // Verify that there are all 3 keys for deletion.
        // In the DistributedCompactor, at first 3 keys will be selected for deletion, and later no keys get filtered out from the deletionList based on the compactionControlTimestamp.
        assertTrue(expanded.getPendingCompaction() != null);
        assertTrue(expanded.getPendingCompaction().getKeysToDelete().size() == 3, "All 3 deltas should up for deletion.");
        assertTrue(ImmutableSet.copyOf(expanded.getPendingCompaction().getKeysToDelete()).equals(ImmutableSet.of(t1, t2, t3)));
    }

    @Test
    public void deltasAfterCompactionControlTimestampShouldOnlyBeDeletedAndDeltasBeforeCompactionControlTimestampShouldNotBeDeleted() {

        long nowInTimeMillis = System.currentTimeMillis();

        UUID t1 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis);
        UUID t2 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 10 * 1000);

        // note: compaction control timestamp is after t1, t2 and before t3
        long compactionControlTimestamp = nowInTimeMillis + 20 * 1000;

        UUID t3 = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 30 * 1000);

        long fullConsistencyTimestamp = nowInTimeMillis + 40 * 1000;

        Delta delta1 = Deltas.literal(ImmutableMap.of("key1", "value1"));
        Delta delta2 = Deltas.literal(ImmutableMap.of("key2", "value2"));
        Delta delta3 = Deltas.mapBuilder().put("key3", "change").build();

        final List<Map.Entry<UUID, Change>> deltas = ImmutableList.of(
                Maps.immutableEntry(t1, ChangeBuilder.just(t1, delta1)),
                Maps.immutableEntry(t2, ChangeBuilder.just(t2, delta2)),
                Maps.immutableEntry(t3, ChangeBuilder.just(t3, delta3)));

        Record record = mock(Record.class);
        final Key key = mock(Key.class);
        when(record.getKey()).thenReturn(key);
        final List<Map.Entry<UUID, Compaction>> compactions = Lists.newArrayList();
        when(record.passOneIterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas.iterator());

        //noinspection unchecked
        Supplier<Record> requeryFn = mock(Supplier.class);
        when(requeryFn.get()).thenReturn(record);

        MetricRegistry metricRegistry = new MetricRegistry();
        Counter archiveDeltaSize = metricRegistry.counter(MetricRegistry.name("bv.emodb.sor", "DefaultCompactor", "archivedDeltaSize"));
        Compactor compactor = new DistributedCompactor(archiveDeltaSize, false, metricRegistry);
        Expanded expanded = compactor.expand(record, fullConsistencyTimestamp, fullConsistencyTimestamp, compactionControlTimestamp, MutableIntrinsics.create(key), false, requeryFn);

        assertTrue(expanded.getPendingCompaction() != null);
        // Do not delete deltas just yet; deltas are going to be deleted now in the next read
        assertTrue(expanded.getPendingCompaction().getKeysToDelete().isEmpty());
        // Add the compactions to the compaction list
        UUID compactionTimestamp = TimeUUIDs.uuidForTimeMillis(nowInTimeMillis + 35 * 1000); // altering this instead of "expanded.getPendingCompaction().getChangeId()"
        compactions.add(Maps.immutableEntry(compactionTimestamp, expanded.getPendingCompaction().getCompaction()));
        when(record.passOneIterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas.iterator());

        expanded = compactor.expand(record, fullConsistencyTimestamp, fullConsistencyTimestamp, compactionControlTimestamp, MutableIntrinsics.create(key), false, requeryFn);

        // Verify that there is exactly 1 key to be deleted
        // In the DistributedCompactor, at first 3 keys will be selected for deletion, and later 2 keys get filtered out based on the compactionControlTimestamp.
        assertTrue(expanded.getPendingCompaction() != null);
        assertTrue(expanded.getPendingCompaction().getKeysToDelete().size() == 1, "Only 1 delta should up for deletion.");
        assertTrue(ImmutableSet.copyOf(expanded.getPendingCompaction().getKeysToDelete()).equals(ImmutableSet.of(t3)));
    }
}