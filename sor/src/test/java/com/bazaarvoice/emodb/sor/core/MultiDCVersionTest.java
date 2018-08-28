package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.db.test.InMemoryDataReaderDAO;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.test.MultiDCDataStores;
import com.bazaarvoice.emodb.sor.test.SystemClock;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertTrue;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

/**
 * Tests that writes in multiple data centers result in a consistent
 * local-data center version number.
 */
public class MultiDCVersionTest {

    private static final String TABLE = "item";
    private static final String KEY = "key1";

    @Test
    public void testCompaction() throws Exception {
        // setup a pair of DataStores that replicate to each other, simulating two data centers
        MultiDCDataStores allDCs = new MultiDCDataStores(2, new MetricRegistry());
        DataStore dc1 = allDCs.dc(0);
        DataStore dc2 = allDCs.dc(1);
        InMemoryDataReaderDAO dao1 = allDCs.dao(0);
        InMemoryDataReaderDAO dao2 = allDCs.dao(1);

        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        dc1.createTable(TABLE, options, Collections.<String, Object>emptyMap(), newAudit("create table"));

        UUID changeId1, changeId2, changeId3, changeId4;

        // write an initial update that's visible to both data centers
        dc1.update(TABLE, KEY, changeId1 = TimeUUIDs.newUUID(), Deltas.fromString("{\"name\":\"Bob\"}"), newAudit("submit"), WriteConsistency.STRONG);

        // pause replication between data centers
        allDCs.stopReplication();

        // moderate in data center 1 and verify the version number
        dc1.update(TABLE, KEY, changeId2 = TimeUUIDs.newUUID(), Deltas.fromString("{..,\"status\":\"APPROVED\"}"), newAudit("moderate"), WriteConsistency.STRONG);
        Map<String, Object> valueA = dc1.get(TABLE, KEY, ReadConsistency.STRONG);
        assertEquals(Intrinsic.getVersion(valueA), (Long) 2L);
        assertEquals(valueA.get("name"), "Bob");
        assertEquals(valueA.get("status"), "APPROVED");

        // concurrently, replace the content completely in a different data center and compact
        dc2.update(TABLE, KEY, changeId3 = TimeUUIDs.newUUID(), Deltas.fromString("{\"name\":\"Tom\"}"), newAudit("resubmit"), WriteConsistency.STRONG);
        dc2.compact(TABLE, KEY, null, ReadConsistency.STRONG, WriteConsistency.STRONG);
        // Note that the above compaction doesn't really compact anything .. the following will and then bad things occur (not in this case though, maybe)
        //dc2.compact(TABLE, KEY, Duration.millis(1), ReadConsistency.STRONG, WriteConsistency.STRONG);
        Map<String, Object> valueB = dc2.get(TABLE, KEY, ReadConsistency.STRONG);
        assertEquals(Intrinsic.getVersion(valueB), (Long) 2L);
        assertEquals(valueB.get("name"), "Tom");
        assertFalse(valueB.containsKey("status"));

        // replicate the changes we just made
        allDCs.startReplication();

        // verify that data store 1 sees the new content from data store 2 with a new version number
        Map<String, Object> valueC = dc1.get(TABLE, KEY, ReadConsistency.STRONG);
        assertEquals(Intrinsic.getVersion(valueC), (Long) 3L);
        assertEquals(valueC.get("name"), "Tom");
        assertFalse(valueC.containsKey("status"));

        // verify that data store 2 sees what it saw before, except with a new version number
        Map<String, Object> valueD = dc1.get(TABLE, KEY, ReadConsistency.STRONG);
        assertEquals(Intrinsic.getVersion(valueD), (Long) 3L);
        assertEquals(valueD.get("name"), "Tom");
        assertFalse(valueD.containsKey("status"));

        // write an update that is considered "old" in DC1 and "new" in DC2, relative to the full consistency timestamp
        dao2.setFullConsistencyTimestamp(SystemClock.tick());
        SystemClock.tick();
        dc1.update(TABLE, KEY, changeId4 = TimeUUIDs.newUUID(), Deltas.fromString("{..,\"status\":\"APPROVED\"}"), newAudit("moderate"), WriteConsistency.STRONG);
        dao1.setFullConsistencyTimestamp(SystemClock.tick());

        allDCs.stopReplication();

        // perform a parallel compaction and verify that we get multiple compaction records
        dc1.compact(TABLE, KEY, null, ReadConsistency.STRONG, WriteConsistency.STRONG);
        dc2.compact(TABLE, KEY, null, ReadConsistency.STRONG, WriteConsistency.STRONG);
        List<Compaction> compactions1 = getCompactions(dc1.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG));
        assertEquals(compactions1.size(), 1);
        assertCompactionEquals(compactions1.get(0), 3+/*cutoff is also compacted now*/1, changeId1, changeId4);
        // We will still see our change Id since the compaction doesn't get rid of deltas
        assertNotNull(asMap(dc1.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG)).get(changeId3));

        List<Compaction> compactions2 = getCompactions(dc2.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG));
        assertEquals(compactions2.size(), 1);
        assertCompactionEquals(compactions2.get(0), 2+1, changeId1, changeId3);
        // We compact cutoff Id into compaction too, but defer the deletes until the compactions are also behind the FCT.
        assertNotNull(asMap(dc2.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG)).get(changeId3));

        allDCs.startReplication();

        // restore replication and verify that each data center gets two compaction records and changeId3 was re-added to DC1
        assertEquals(getCompactions(dc1.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG)).size(), 2);
        assertEquals(getCompactions(dc2.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG)).size(), 2);
        assertTrue(cutoffExists(asMap(dc1.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG)), changeId3));

        // now perform a get that should detect the multiple records and compact them (MultiDCDataStores uses the sameThreadExecutor by default)
        Map<String, Object> actualRow = dc1.get(TABLE, KEY, ReadConsistency.STRONG);
        compactions1 = getCompactions(dc1.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG));
        // We will have two compactions since we defer the delete of the base compaction until the owning compaction is behind the FCT
        assertEquals(compactions1.size(), 2);

        assertCompactionEquals(compactions1.get(0), 3/*add back cutoof delta*/+1, changeId1, changeId4);

        // Make sure the content and version is as expected
        assertEquals(Intrinsic.getVersion(actualRow), (Long) 4L);
        assertEquals(actualRow.get("name"), "Tom");
        assertTrue(actualRow.containsKey("status"));

        // Note that compaction-owned deltas are still not deleted, and will only get deleted when the FCT time
        // includes their respective compactions.
        assertNotNull(asMap(dc1.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG)).get(changeId3));
        assertNotNull(asMap(dc2.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG)).get(changeId3));

        // Let's set the FCT right before current compaction timestamp, so there are deltas that are before FCT, but an outstanding compaction
        // exists. And so, no compaction should occur.

        UUID compactionId1 = getCompactionsWithId(dc1.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG))
                .keySet().toArray(new UUID[0])[0];
        UUID justAfterCutoff = TimeUUIDs.getNext(compactions1.get(0).getCutoff());
        dao1.setFullConsistencyTimestamp(TimeUUIDs.getTimeMillis(justAfterCutoff));

        // Now get the key, and verify that no compaction occurred.
        dc1.get(TABLE, KEY, ReadConsistency.STRONG);
        Map<UUID, Compaction> compactionMap1 =
                getCompactionsWithId(dc1.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG));
        assertEquals(compactionMap1.size(), 2);
        assertEquals(compactionMap1.keySet().toArray(new UUID[0])[0], compactionId1); // still has the old Id.
        // Verify the deltas are not yet deleted
        assertNotNull(asMap(dc1.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG)).get(changeId3));

        // Now, update the FCT to cover compaction
        dao1.setFullConsistencyTimestamp(SystemClock.tick());
        dc1.get(TABLE, KEY, ReadConsistency.STRONG);
        // Verify the deltas are deleted this time around
        assertNull(Maps.filterEntries(asMap(dc1.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG)), entry -> entry.getValue().getHistory() == null).get(changeId3));
        // Verify there is only one compaction record
        compactionMap1 =
                getCompactionsWithId(dc1.getTimeline(TABLE, KEY, true, false, null, null, false, 100, ReadConsistency.STRONG));
        assertEquals(compactionMap1.size(), 1);
    }

    private void assertCompactionEquals(Compaction actual, int count, UUID first, UUID cutoff) {
        assertEquals(actual.getCount(), count);
        assertEquals(actual.getFirst(), first);
        assertEquals(actual.getCutoff(), cutoff);
    }

    private Audit newAudit(String comment) {
        return new AuditBuilder().
                setProgram("test").
                setLocalHost().
                setComment(comment).
                build();
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

    private Map<UUID, Compaction> getCompactionsWithId(Iterator<Change> changeIter) {
        Map<UUID, Compaction> compactions = Maps.newHashMap();
        while (changeIter.hasNext()) {
            Change change = changeIter.next();
            if (change.getCompaction() != null) {
                compactions.put(change.getId(), change.getCompaction());
            }
        }
        return compactions;
    }

    private boolean cutoffExists(Map<UUID, Change> timeline, UUID cutoffId) {
        for(Change change : timeline.values()) {
            Compaction compaction;
            if ((compaction = change.getCompaction()) != null &&
                    TimeUUIDs.compare(compaction.getCutoff(), cutoffId) == 0) {
                return true;
            }
        }
        return false;
    }

    private Map<UUID, Change> asMap(Iterator<Change> changeIter) {
        Map<UUID, Change> map = Maps.newLinkedHashMap();
        while (changeIter.hasNext()) {
            Change change = changeIter.next();
            map.put(change.getId(), change);
        }
        return map;
    }
}
