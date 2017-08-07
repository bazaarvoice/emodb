package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.test.InMemoryDataDAO;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.test.MultiDCDataStores;
import com.bazaarvoice.emodb.sor.test.SystemClock;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests that writes in multiple data centers result in a consistent
 * local-data center version number.
 */
public class DeltaHistoryTest {

    private static final String TABLE = "item";
    private static final String KEY = "key";
    private static final String KEY2 = "key2";

    @Test
    public void testDeltaAudits() {
        MetricRegistry metricRegistry = new MetricRegistry();

        // Just use one datacenter for this test
        MultiDCDataStores allDCs = new MultiDCDataStores(1, metricRegistry);
        DataStore dc1 = allDCs.dc(0);
        InMemoryDataDAO dao1 = allDCs.dao(0);

        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        dc1.createTable(TABLE, options, Collections.<String, Object>emptyMap(), newAudit("create table"));

        // Write an initial update
        // Version 1
        dc1.update(TABLE, KEY, TimeUUIDs.newUUID(), Deltas.fromString("{\"name\":\"Bob\"}"), newAudit("submit"), WriteConsistency.STRONG);

        // Version 2
        dc1.update(TABLE, KEY, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"status\":\"APPROVED\"}"), newAudit("moderate"), WriteConsistency.STRONG);

        // Version 3
        dc1.update(TABLE, KEY, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"code\":\"s\"}"), newAudit("moderate"), WriteConsistency.STRONG);

        // Update FullConsistency time again
        dao1.setFullConsistencyTimestamp(SystemClock.tick());
        SystemClock.tick();

        // Versions 1, 2 & 3 should get compacted
        dc1.compact(TABLE, KEY, null, ReadConsistency.STRONG, WriteConsistency.STRONG);

        // Version 4
        dc1.update(TABLE, KEY, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"newcode\":\"s\"}"), newAudit("moderate"), WriteConsistency.STRONG);

        // Test if versions are consistent even when we audit compaction cutoff deltas

        // Update FullConsistency time again
        dao1.setFullConsistencyTimestamp(SystemClock.tick());
        SystemClock.tick();

        // Version 4 should be compacted
        dc1.compact(TABLE, KEY, null, ReadConsistency.STRONG, WriteConsistency.STRONG);

        // Version 5
        dc1.update(TABLE, KEY, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"newcode\":\"s\"}"), newAudit("moderate"), WriteConsistency.STRONG);

        // Compare the above with the delta audits and make sure they display the correct story

        Iterator<Change> deltaHistory = allDCs.auditStore(0).getDeltaAudits(TABLE, KEY);

        final Iterator<Change> liveChanges = dc1.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG);
        Iterator<Change> allChangesEver = getChangeIterator(deltaHistory, liveChanges);
        MutableIntrinsics intrinsics = MutableIntrinsics.create(new Key(allDCs.tableDao().get(TABLE), KEY));
        Resolver resolver = new DefaultResolver(intrinsics);

        Map<UUID, Change> map = Maps.newLinkedHashMap();
        int countOfAuditedDeltas = 0;
        int version = 0;
        while (allChangesEver.hasNext()) {
            Change change = allChangesEver.next();
            if (change.getHistory() != null) {
                countOfAuditedDeltas++;
                assertEquals(++version, Long.parseLong(change.getHistory().getContent().get("~version").toString()),
                "Version continuity failed");
            }
            map.put(change.getId(), change);
        }

        // Note that it is possible for the same changeId to exist in both archive and in the actual record.
        // This is because we pro-actively archive a delta in anticipation of its future delete, but do *not* delete it right away
        // so that all nodes get a chance to get the corresponding compaction before proceeding with the deletes.
        // Because of this, we need to make sure that for this test, we don't count a delta twice (once from archives, and once from the
        // actual delta). In practice, when actual resolving occurs, a changeId is unique and appears only once.
        for (Map.Entry<UUID, Change> entry : map.entrySet()) {
            resolver.update(entry.getKey(), entry.getValue().getDelta(), entry.getValue().getTags());
        }

        // Verify we have 4 audited deltas
        assertEquals(countOfAuditedDeltas, 4, "There should be 4 deltas stored in audits that got compacted away.");

        // Verify we have a total of 5 distinct changes.
        assertEquals(5, map.size(), "We should have 5 distinct changes");

        // Get the content resolved from all deltas including the audit deltas
        Map<String, Object> deltaContent = ((DefaultDataStore)dc1).toContent(resolver.resolved(), ReadConsistency.STRONG);

        // Get the actual content
        Map<String, Object> actualContent = dc1.get(TABLE, KEY, ReadConsistency.STRONG);

        // Make sure both content are same
        assertTrue(Maps.difference(deltaContent, actualContent).areEqual());

        // Verify that we don't save deltas that are too huge so we don't blow out the memory

        // Generate a delta of about 10MB in size
        long bufferSize = AbstractCompactor.MAX_DELTA_ARCHIVE_SIZE;
        StringBuilder sb = new StringBuilder((int) bufferSize);
        for (long i=0; i < bufferSize; i++) {
            sb.append("a");
        }

        // Version 6
        dc1.update(TABLE, KEY, TimeUUIDs.newUUID(), Deltas.fromString(format("{..,\"bigtextfield\":\"%s\"}", sb.toString())),
                newAudit("moderate"), WriteConsistency.STRONG);

        // Version 7
        dc1.update(TABLE, KEY, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"newcodes\":\"ss\"}"), newAudit("moderate"), WriteConsistency.STRONG);

        // Update FullConsistency time again
        dao1.setFullConsistencyTimestamp(SystemClock.tick());
        SystemClock.tick();

        // Versions 6, and 7 should get compacted, but due to size limit, it should not save the history
        dc1.compact(TABLE, KEY, null, ReadConsistency.STRONG, WriteConsistency.STRONG);

        // Verify that we have the same number of historical deltas as before
        int countOfDeltasAfterHugeDelta = Iterators.advance(allDCs.auditStore(0).getDeltaAudits(TABLE, KEY),
                Integer.MAX_VALUE);

        assertEquals(countOfAuditedDeltas, countOfDeltasAfterHugeDelta,
                "The number of audited deltas should always stay the same once a huge delta is added");

        // Make sure that the archived delta size goes back to 0
        assertEquals(((DefaultDataStore)dc1)._archiveDeltaSize.getCount(), 0L, "The archived delta counter should be back to 0");

    }

    /**
     * If a row gets compacted after a long time, then it is possible that we will try to archive a lot of deltas.
     * In this scenario, it is possible that the collective delta history we will be trying to write out to Cassandra
     * will exceed the thrift limit. We should give up on archiving delta history in that scenario.
     */
    @Test
    public void testDeltaHistoryDisabledIfTooLarge() {
        // Add a delta that is 1MB, which is small enough to pass the delta size test.
        // Then, add very small deltas 10 times, which means it will be over 10 MB thrift limit by the time of compaction,
        // and delta history should be disabled

        // Setup
        MetricRegistry metricRegistry = new MetricRegistry();
        MultiDCDataStores allDCs = new MultiDCDataStores(1, metricRegistry);
        DataStore dc1 = allDCs.dc(0);
        InMemoryDataDAO dao1 = allDCs.dao(0);
        // Reset the archivedDeltaSize static counter to zero - This may have accrued some value due to our use of
        // DiscardingExecutorService in other tests.
        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        dc1.createTable(TABLE, options, Collections.<String, Object>emptyMap(), newAudit("create table"));

        long bufferSize = 1 * 1024L * 1024L; // 1 MB
        StringBuilder sb = new StringBuilder((int) bufferSize);
        for (int i=0; i < bufferSize; i++) {
            sb.append("a");
        }
        dc1.update(TABLE, KEY2, TimeUUIDs.newUUID(), Deltas.fromString(format("{..,\"bigtextfield\":\"%s\"}", sb.toString())),
                newAudit("moderate"), WriteConsistency.STRONG);

        // Now we simply add 10 small deltas
        for (int i = 0; i < 11; i++) {
            dc1.update(TABLE, KEY2, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"newcodes\":\"ss\"}"),
                    newAudit("moderate"), WriteConsistency.STRONG);
        }

        // Update FullConsistency time again
        dao1.setFullConsistencyTimestamp(SystemClock.tick());
        SystemClock.tick();

        // Now make sure that there are no audited deltas since the overall content would be 10MB or more
        // Compaction will occur, but due to transport size limit, it should not save the history
        dc1.compact(TABLE, KEY2, null, ReadConsistency.STRONG, WriteConsistency.STRONG);
        int countOfDeltasAfterHugeDelta = Iterators.advance(allDCs.auditStore(0).getDeltaAudits(TABLE, KEY2), Integer.MAX_VALUE);

        // Since we know that our first delta was 1 MB, the next 10 deltas each will have the content size of at least 1 MB
        // for a total of 10 MB. This is because we keep a "snapshot" of row with each delta archive.
        // Verify that we have not saved any delta history for this compaction.

        assertEquals(countOfDeltasAfterHugeDelta, 0,
                "There should be no audited deltas since, the total size of delta history has crossed thrift limit");

        // Make sure that the archived delta size goes back to 0
        assertEquals(((DefaultDataStore)dc1)._archiveDeltaSize.getCount(), 0L, "The archived delta counter should be back to 0");
    }

    /** This test simulates heavy reads that will force the compaction thread to discard some compactions.
     *  Verify afterwards that our counter goes back to zero even for discarded compactions
     */
    @Test
    public void testDiscardedCompactions()
            throws InterruptedException {
        MetricRegistry metricRegistry = new MetricRegistry();

        // Test Async compaction
        MultiDCDataStores allDCs = new MultiDCDataStores(1, true, metricRegistry);
        final DataStore dc1 = allDCs.dc(0);
        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        dc1.createTable(TABLE, options, Collections.<String, Object>emptyMap(), newAudit("create table"));
        InMemoryDataDAO dao1 = allDCs.dao(0);
        int numberOfConcurrentGets = 1000;
        for (int i = 0; i < numberOfConcurrentGets ; i++) {
            createCompactibleRecord(dc1, dao1, "key" + i);
        }

        // Now that we know we have the above compactible records, ready to get compacted, let's GET them all at once.
        // Hopefully, some of them will get rejected as the queue can only hold one record at a time.
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        // Concurrently call gets so that async compaction is called
        final CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < numberOfConcurrentGets; i++) {
            final int freezeLocalVar = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        // Ignore for the purpose of this test
                    }
                    dc1.get(TABLE, "key" + freezeLocalVar, ReadConsistency.STRONG);
                }
                });
        }
        latch.countDown();

        executorService.awaitTermination(5000L, TimeUnit.MILLISECONDS);

        // Make sure that some of the rows still are left uncompacted, i.e., were discarded for compaction
        boolean uncompactedRecordsFound = false;
        for (int i=0; i < numberOfConcurrentGets; i++) {
            if (!isCompacted(dc1, TABLE, "key" + i)) {
                uncompactedRecordsFound = true;
                break;
            }
        }

        assertTrue(uncompactedRecordsFound, "No rows had discarded compactions. Can't continue this test. " +
                "Try increasing the number of concurrent gets.");

        // Make sure that the delta metric is back to 0, even though compactions were discarded
        assertEquals(((DefaultDataStore)dc1)._archiveDeltaSize.getCount(), 0L, "The archived delta counter should be back to 0");
    }

    private boolean isCompacted(DataStore ds, String table, String key) {
        Iterator<Change> changes = ds.getTimeline(table, key, true, false, null, null, false, 100, ReadConsistency.STRONG);
        while (changes.hasNext()) {
            if (changes.next().getCompaction() != null) {
                return true;
            }
        }
        return false;
    }

    private void createCompactibleRecord(DataStore dc1, InMemoryDataDAO dao1, String key) {
        // Write an initial update
        // Version 1
        dc1.update(TABLE, key, TimeUUIDs.newUUID(), Deltas.fromString("{\"name\":\"Bob\"}"), newAudit("submit"), WriteConsistency.STRONG);

        // Version 2
        dc1.update(TABLE, key, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"status\":\"APPROVED\"}"), newAudit("moderate"), WriteConsistency.STRONG);

        // Update FullConsistency time again
        // So now version 1  should get compacted into version 2 on next read
        dao1.setFullConsistencyTimestamp(SystemClock.tick());
        SystemClock.tick();
    }

    private Iterator<Change> getChangeIterator(Iterator<Change> deltaHistory, final Iterator<Change> liveChanges) {
        return Iterators.concat(deltaHistory, new AbstractIterator<Change>() {
            @Override
            protected Change computeNext() {
                while (liveChanges.hasNext()) {
                    Change change = liveChanges.next();
                    if (change.getDelta() != null) {
                        return change;
                    }
                }
                return endOfData();
            }
        });
    }

    private Audit newAudit(String comment) {
        return new AuditBuilder().
                setProgram("test").
                setLocalHost().
                setComment(comment).
                build();
    }
}
