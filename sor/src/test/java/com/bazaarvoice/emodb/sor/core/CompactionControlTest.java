package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.ChangeBuilder;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.test.SystemClock;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class CompactionControlTest {

    @Test
    public void deltasBeforeCompactionControlTimestampShouldBeDeletedLikeBefore() {
        final Key key = mock(Key.class);
        UUID t1 = TimeUUIDs.newUUID();
        UUID t2 = TimeUUIDs.newUUID();
        UUID t3 = TimeUUIDs.newUUID();

        UUID t4 = TimeUUIDs.newUUID();
        UUID t5 = TimeUUIDs.newUUID();
        UUID t6 = TimeUUIDs.newUUID();

        SystemClock.tick();

        Delta delta1 = Deltas.literal(ImmutableMap.of("key1", "value1"));
        Delta delta2 = Deltas.literal(ImmutableMap.of("key2", "value2"));
        Delta delta3 = Deltas.mapBuilder().put("key3", "change").build();
        Compaction compaction1 = new Compaction(2, t4, t6, "abcdef0123456789", t6, t6, delta1);

        final List<Map.Entry<UUID, Compaction>> compactions = ImmutableList.of(
                Maps.immutableEntry(t4, compaction1));

        final List<Map.Entry<UUID, Change>> deltas2 = ImmutableList.of(
                Maps.immutableEntry(t1, ChangeBuilder.just(t1, delta2)),
                Maps.immutableEntry(t2, ChangeBuilder.just(t2, delta2)),
                Maps.immutableEntry(t3, ChangeBuilder.just(t3, delta3)));

        Record record = mock(Record.class);
        when(record.getKey()).thenReturn(key);
        when(record.passOneIterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(deltas2.iterator());

        //noinspection unchecked
        Supplier<Record> requeryFn = mock(Supplier.class);
        when(requeryFn.get()).thenReturn(record);

        long now = System.currentTimeMillis();
        long compactionControlTimestamp = now;
        MetricRegistry metricRegistry = new MetricRegistry();
        Counter archiveDeltaSize = metricRegistry.counter(MetricRegistry.name("bv.emodb.sor", "DefaultCompactor", "archivedDeltaSize"));
        Expanded expanded = new DistributedCompactor(archiveDeltaSize, false, metricRegistry)
                .expand(record, now, now, compactionControlTimestamp, MutableIntrinsics.create(key), false, requeryFn);

        // Verify that there are exactly 3 keys that are to be deleted
        assertTrue(expanded.getPendingCompaction() != null);
        assertEquals(expanded.getPendingCompaction().getKeysToDelete().size(), 3);
    }

    @Test
    public void deltasAfterCompactionControlTimestampShouldNotBeDeleted() {
        final Key key = mock(Key.class);
        UUID t1 = TimeUUIDs.newUUID();
        UUID t2 = TimeUUIDs.newUUID();
        SystemClock.tick();
        long compactionControlTimestamp = System.currentTimeMillis();
        SystemClock.tick();
        UUID t3 = TimeUUIDs.newUUID();
        UUID t4 = TimeUUIDs.newUUID();
        UUID t5 = TimeUUIDs.newUUID();
        UUID t6 = TimeUUIDs.newUUID();

        SystemClock.tick();

        Delta delta1 = Deltas.literal(ImmutableMap.of("key1", "value1"));
        Delta delta2 = Deltas.literal(ImmutableMap.of("key2", "value2"));
        Delta delta3 = Deltas.mapBuilder().put("key3", "change").build();
        Compaction compaction1 = new Compaction(2, t4, t6, "abcdef0123456789", t6, t6, delta1);

        final List<Map.Entry<UUID, Compaction>> compactions = ImmutableList.of(
                Maps.immutableEntry(t4, compaction1));

        final List<Map.Entry<UUID, Change>> deltas2 = ImmutableList.of(
                Maps.immutableEntry(t1, ChangeBuilder.just(t1, delta2)),
                Maps.immutableEntry(t2, ChangeBuilder.just(t2, delta2)),
                Maps.immutableEntry(t3, ChangeBuilder.just(t3, delta3)));

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
        Expanded expanded = new DistributedCompactor(archiveDeltaSize, false, metricRegistry)
                .expand(record, now, now, compactionControlTimestamp, MutableIntrinsics.create(key), false, requeryFn);

        // Verify that there are exactly 2(3-1) keys that are to be deleted
        // In the DistributedCompactor, at first 3 keys will be selected for deletion, and later 1 get filtered out based on the compactionControlTimestamp.
        assertTrue(expanded.getPendingCompaction() != null);
        assertEquals(expanded.getPendingCompaction().getKeysToDelete().size(), 2);
    }
}
