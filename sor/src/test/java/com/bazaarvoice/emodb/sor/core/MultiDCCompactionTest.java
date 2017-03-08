package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.common.uuid.UUIDs;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.db.test.InMemoryDataDAO;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.test.MultiDCDataStores;
import com.bazaarvoice.emodb.sor.test.SystemClock;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.joda.time.Duration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Tests that compactions in multiple data centers do not result in data loss.
 * New compactions should not delete the base compaction unless the new "owner" compaction is also behind the FCT.
 */
public class MultiDCCompactionTest {

    private static final String TABLE = "item";
    private static final String KEY = "key1";

    @Test
    public void testMultiDcCompactionWithDelayBetweenDeletesAndCompaction() throws Exception {
        // setup a pair of DataStores that replicate to each other, simulating two data centers
        MultiDCDataStores allDCs = new MultiDCDataStores(2, new MetricRegistry());
        DataStore dc1 = allDCs.dc(0);
        DataStore dc2 = allDCs.dc(1);
        InMemoryDataDAO dao1 = allDCs.dao(0);
        InMemoryDataDAO dao2 = allDCs.dao(1);

        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        dc1.createTable(TABLE, options, Collections.<String, Object>emptyMap(), newAudit("create table"));

        UUID changeId1, changeId2, changeId3, changeId4, changeId5;

        // write initial updates that's visible to both data centers
        dc1.update(TABLE, KEY, changeId1 = TimeUUIDs.newUUID(), Deltas.fromString("{\"first_name\":\"Bob\"}"), newAudit("submit"), WriteConsistency.STRONG);
        dc1.update(TABLE, KEY, changeId2 = TimeUUIDs.newUUID(), Deltas.fromString("{..,\"last_name\":\"Smith\"}"), newAudit("update"), WriteConsistency.STRONG);

        SystemClock.tick();

        // This should add a new compaction record while deferring the deletes upon a subsequent compaction
        dc1.compact(TABLE, KEY, Duration.millis(0), ReadConsistency.STRONG, WriteConsistency.STRONG);

        SystemClock.tick();
        // write 3 more deltas that are replicated everywhere
        dc1.update(TABLE, KEY, changeId3 = TimeUUIDs.newUUID(), Deltas.fromString("{..,\"third\":\"555-5555\"}"), newAudit("update"), WriteConsistency.STRONG);
        SystemClock.tick();
        UUID cutoffchangeId = TimeUUIDs.getNext(changeId3);
        dc1.update(TABLE, KEY, changeId4 = TimeUUIDs.newUUID(), Deltas.fromString("{..,\"fourth\":\"more\"}"), newAudit("update"), WriteConsistency.STRONG);
        SystemClock.tick();
        dc1.update(TABLE, KEY, changeId5 = TimeUUIDs.newUUID(), Deltas.fromString("{..,\"fifth\":\"more\"}"), newAudit("update"), WriteConsistency.STRONG);

        // Simulate an out of order replication, where the deletes associated with the compaction will replicate,
        // but the corresponding compaction record itself won't replicate until later
        allDCs.onlyReplicateDeletesUponCompactions();

        SystemClock.tick();
        // This will *only* replicate the deletes associated with the compaction
        dc1.compact(TABLE, KEY, Duration.millis(System.currentTimeMillis() - TimeUUIDs.getTimeMillis(cutoffchangeId)), ReadConsistency.STRONG, WriteConsistency.STRONG);

        // DC2 has replicated the deletes for first two deltas, and the first compaction, but didn't get the second compaction delta.
        // Now, DC2 undergoes a compaction on its own
        // This will cause data loss if the second compaction deleted the first compaction too.
        dc2.compact(TABLE, KEY, Duration.millis(0), ReadConsistency.STRONG, WriteConsistency.STRONG);

        // Let's replicate the compaction records
        allDCs.replicateCompactionDeltas();

        Map<String, Object> actualRow = dc1.get(TABLE, KEY, ReadConsistency.STRONG);
        Map<String, Object> actualRowdc2 = dc2.get(TABLE, KEY, ReadConsistency.STRONG);

        // Verify that the record in both data centers has converged
        assertEquals(actualRow, actualRowdc2, "Both rows should converge at this point");

        // Verify that the first two deltas are now gone !!
        assertTrue(actualRow.containsKey("first_name"));
        assertTrue(actualRow.containsKey("last_name"));
        assertTrue(actualRow.containsKey("third"));
        assertTrue(actualRow.containsKey("fourth"));
        assertTrue(actualRow.containsKey("fifth"));
    }

    /** Test that compactions are only deleted when the chosen compaction is behind FCT. **/
    @Test
    public void testCorrectCompactionsAreDeleted() {
        // Test that compactions are only deleted when the chosen compaction is behind FCT.
        // As it is, a new compaction is not started if a compaction exists outside of FCT.
        // So, when the most effective compaction is found to be behind FCT, only then every other compaction is deleted.
        // If the winning compaction happens to be outside of FCT, we defer the deletes of *all* other compactions.

        // Create a compactor
        long now = System.currentTimeMillis();
        MetricRegistry metricRegistry = new MetricRegistry();
        Counter archiveDeltaSize = metricRegistry.counter(MetricRegistry.name("bv.emodb.sor", "DistributedCompactor", "testArchivedDeltaSize"));
        Compactor compactor = new DistributedCompactor(archiveDeltaSize, false, metricRegistry);

        final Key key = mock(Key.class);
        UUID t1 = TimeUUIDs.newUUID(); // First compaction
        SystemClock.tick();
        long fctBeforeT2 = System.currentTimeMillis();
        SystemClock.tick();
        UUID t2 = TimeUUIDs.newUUID(); // Second compaction (winning)
        SystemClock.tick();
        long fctAfterT2 = System.currentTimeMillis();
        SystemClock.tick();
        UUID t3 = TimeUUIDs.newUUID(); // Third compaction

        // Sample content
        Delta delta = Deltas.literal(ImmutableMap.of("key", "value"));

        // Mock compaction signature
        Hasher hasher = Hashing.md5().newHasher();
        hasher.putBytes(UUIDs.asByteArray(t1));
        String signature = hasher.hash().toString();

        // c2 is the winning compaction
        Compaction c1 = new Compaction(4, t1, t1, signature, null, null, delta);
        Compaction c2 = new Compaction(4, t1, t1, signature, null, null, delta);
        Compaction c3 = new Compaction(3, t1, t1, signature, null, null, delta);

        final List<Map.Entry<UUID, Compaction>> compactions = Lists.newArrayList(
                Maps.immutableEntry(t1, c1),
                Maps.immutableEntry(t2, c2),
                Maps.immutableEntry(t3, c3)
                );

        Record record = mock(Record.class);
        when(record.getKey()).thenReturn(key);
        when(record.passOneIterator()).thenReturn(compactions.iterator()).thenReturn(compactions.iterator());
        when(record.passTwoIterator()).thenReturn(Iterators.emptyIterator()).thenReturn(Iterators.emptyIterator());

        //noinspection unchecked
        Supplier<Record> requeryFn = mock(Supplier.class);

        Expanded expand = compactor.expand(record, fctBeforeT2, fctBeforeT2, Long.MIN_VALUE, MutableIntrinsics.create(key), false, requeryFn);

        // Verify that no pending compaction is produced since the winning compaction is outside of FCT
        assertNull(expand.getPendingCompaction());

        // Change the FCT such that the winning compaction (c2) is before FCT
        expand = compactor.expand(record, fctAfterT2, fctAfterT2, Long.MIN_VALUE, MutableIntrinsics.create(key), false, requeryFn);
        List<UUID> deletedCompactions = expand.getPendingCompaction().getCompactionKeysToDelete();
        // Verify that compactions other than c2, are deleted
        Assert.assertTrue(deletedCompactions.contains(t1));
        Assert.assertTrue(deletedCompactions.contains(t3));
    }

    private Audit newAudit(String comment) {
        return new AuditBuilder().
                setProgram("test").
                setLocalHost().
                setComment(comment).
                build();
    }
}
