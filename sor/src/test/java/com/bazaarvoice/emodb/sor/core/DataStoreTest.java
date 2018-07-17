package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.test.SystemClock;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class DataStoreTest {

    private static final String TABLE = "item";
    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";

    @Test
    public void testDeltas() throws Exception {
        DataStore store = new InMemoryDataStore(new MetricRegistry());

        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        assertFalse(store.getTableExists(TABLE));
        store.createTable(TABLE, options, Collections.<String,Object >emptyMap(), newAudit("create table"));
        assertTrue(store.getTableExists(TABLE));

        Date start = new Date();

        // write some data
        store.update(TABLE, KEY1, TimeUUIDs.newUUID(), Deltas.fromString("{\"name\":\"Bob\"}"), newAudit("submit"), WriteConsistency.STRONG);
        store.update(TABLE, KEY1, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"state\":\"SUBMITTED\"}"), newAudit("begin moderation"), WriteConsistency.STRONG);
        store.update(TABLE, KEY2, TimeUUIDs.newUUID(), Deltas.fromString("{\"name\":\"Joe\"}"), newAudit("submit"), WriteConsistency.STRONG);
        // Tag this last update
        store.updateAll(
                ImmutableList.of(
                        new Update(TABLE, KEY1, TimeUUIDs.newUUID(), Deltas.fromString("{..,\"state\":\"APPROVED\"}"), newAudit("finish moderation"), WriteConsistency.STRONG)),
                ImmutableSet.of("tag2", "tag1"));

        Date end = new Date(System.currentTimeMillis() + 1000);  // TimeUUID generation may result in dates that are too new by a very small amount

        // verify everything we have written about key1
        Map<String, Object> content1 = store.get(TABLE, KEY1, ReadConsistency.STRONG);
        ImmutableMap<Object, Object> content1Expected = ImmutableMap.builder().
                put("~id", KEY1).
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
        assertTrue(Intrinsic.getSignature(content1).matches("[0-9a-f]{32}"));
        assertTrue(start.compareTo(Intrinsic.getFirstUpdateAt(content1)) <= 0);
        assertTrue((Intrinsic.getFirstUpdateAt(content1)).compareTo(Intrinsic.getLastUpdateAt(content1)) <= 0);
        assertTrue(end.compareTo(Intrinsic.getLastUpdateAt(content1)) >= 0);

        // verify everything we have written about key2
        Map<String, Object> content2 = store.get(TABLE, KEY2, ReadConsistency.STRONG);
        assertEquals(content2, ImmutableMap.builder().
                put("~id", KEY2).
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
        Map<String, Object> content3 = store.get(TABLE, "key3", ReadConsistency.STRONG);
        assertEquals(content3, ImmutableMap.builder().
                put("~id", "key3").
                put("~table", TABLE).
                put("~version", 0L).
                put("~deleted", true).
                put("~signature", "00000000000000000000000000000000").
                build());

        // now delete key2 and verify that the delete has the expected effect
        store.update(TABLE, KEY2, TimeUUIDs.newUUID(), Deltas.fromString("~"), newAudit("delete"), WriteConsistency.STRONG);
        Map<String, Object> content2Deleted = store.get(TABLE, KEY2, ReadConsistency.STRONG);
        ImmutableMap<Object, Object> content2DeletedExpected = ImmutableMap.builder().
                put("~id", KEY2).
                put("~table", TABLE).
                put("~version", 2L).
                put("~deleted", true).
                put("~signature", Intrinsic.getSignature(content2Deleted)).
                put("~firstUpdateAt", content2.get(Intrinsic.FIRST_UPDATE_AT)).
                put("~lastUpdateAt", content2Deleted.get(Intrinsic.LAST_UPDATE_AT)).
                put("~lastMutateAt", content2Deleted.get(Intrinsic.LAST_MUTATE_AT)).
                build();
        assertEquals(content2Deleted, content2DeletedExpected);
        assertTrue(Intrinsic.getSignature(content2Deleted).matches("[0-9a-f]{32}"));
        assertNotEquals(Intrinsic.getSignature(content2), Intrinsic.getSignature(content2Deleted));
        assertNotEquals(Intrinsic.getSignature(content2Deleted), "00000000000000000000000000000000");
        assertTrue((Intrinsic.getLastUpdateAt(content2Deleted)).compareTo(Intrinsic.getLastUpdateAt(content2)) >= 0);

        // try to compact key1 with a long "full-consistency" ttl.  this should have no effect because no deltas are old enough to compact.
        store.compact(TABLE, KEY1, Duration.ofDays(365), ReadConsistency.STRONG, WriteConsistency.STRONG);
        assertEquals(getDeltas(store.getTimeline(TABLE, KEY1, true, false, null, null, false, 100, ReadConsistency.STRONG)).size(), 3);

        // try again to compact key1, this time assuming full consistency has been achieved.  verify compaction doesn't change the content.
        store.compact(TABLE, KEY1, Duration.ZERO, ReadConsistency.STRONG, WriteConsistency.STRONG);
        // This will result in compaction, but no deltas will be deleted since compaction itself is not within FCT.
        // Note that this is the first time it will compact, the compaction is not going to delete the compacted deltas, just create a new compaction
        assertEquals(getDeltas(store.getTimeline(TABLE, KEY1, true, false, null, null, false, 100, ReadConsistency.STRONG)).size(), 3);
        assertEquals(getCompactions(store.getTimeline(TABLE, KEY1, true, false, null, null, false, 100, ReadConsistency.STRONG)).size(), 1);
        // Now compact the record one more time assuming full consistency - this time no new compactions will be created, but the deltas will be deleted
        SystemClock.tick();
        store.compact(TABLE, KEY1, Duration.ZERO, ReadConsistency.STRONG, WriteConsistency.STRONG);
        // This will result in no deltas, just one compaction record that includes the resolved content
        assertEquals(getDeltas(store.getTimeline(TABLE, KEY1, true, false, null, null, false, 100, ReadConsistency.STRONG)).size(), 0);
        // Verify that we have one compaction with compacted delta
        assertEquals(getCompactions(store.getTimeline(TABLE, KEY1, true, false, null, null, false, 100, ReadConsistency.STRONG)).size(), 1);
        assertEquals(store.get(TABLE, KEY1, ReadConsistency.STRONG), content1Expected);

        // try to compact key2 (which was deleted) with a long "full-consistency" ttl.  this should have no effect because no deltas are old enough to compact.
        store.compact(TABLE, KEY2, Duration.ofDays(365), ReadConsistency.STRONG, WriteConsistency.STRONG);
        assertEquals(getDeltas(store.getTimeline(TABLE, KEY2, true, false, null, null, false, 100, ReadConsistency.STRONG)).size(), 2);
        assertEquals(store.get(TABLE, KEY2, ReadConsistency.STRONG), content2DeletedExpected);

        // version numbers can't be trusted when using anything less than STRONG read consistency
        assertEquals(Intrinsic.getVersion(store.get(TABLE, KEY1, ReadConsistency.STRONG)), (Long) 3L);
        assertFalse(store.get(TABLE, KEY1, ReadConsistency.WEAK).containsKey(Intrinsic.VERSION));

        // verify that we retrieve the table placements correctly
        Set<String> expectedTablesPlacements = Sets.newHashSet("app_global:default", "ugc_global:ugc", "app_global:sys", "catalog_global:cat");
        assertTrue(store.getTablePlacements().containsAll(expectedTablesPlacements));
        assertTrue(expectedTablesPlacements.containsAll(store.getTablePlacements()));

        // verify the timeline for key1
        List<Audit> timeline = getAudits(
                store.getTimeline(TABLE, KEY1, true, true, null, null, false, 100, ReadConsistency.STRONG));
        assertEquals(timeline.size(), 3);
        assertEquals(timeline.get(0).getComment(), "submit");
        assertEquals(timeline.get(0).getTags(), ImmutableList.of());
        assertEquals(timeline.get(1).getComment(), "begin moderation");
        assertEquals(timeline.get(1).getTags(), ImmutableList.of());
        assertEquals(timeline.get(2).getComment(), "finish moderation");
        assertEquals(timeline.get(2).getTags(), ImmutableList.of("tag1", "tag2"));
    }

    @Test
    public void testRecordTimestamps() throws Exception {
        DataStore store = new InMemoryDataStore(new MetricRegistry());

        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        assertFalse(store.getTableExists(TABLE));
        store.createTable(TABLE, options, Collections.<String,Object >emptyMap(), newAudit("create table"));
        assertTrue(store.getTableExists(TABLE));

        // Record does not exist initially, so all dates should not be present
        Map<String, Object> record = store.get(TABLE, KEY1);
        assertTrue(Intrinsic.isDeleted(record));
        assertNull(Intrinsic.getFirstUpdateAt(record));
        assertNull(Intrinsic.getLastUpdateAt(record));
        assertNull(Intrinsic.getLastMutateAt(record));

        Audit audit = newAudit("test");

        Date now = new Date(System.currentTimeMillis() - 5000);
        // Normally using uuidForTimestamp is a bad idea, but for this test we need precise control over the UUID timestamps.
        // So long as no two are created with the same time we'll be ok.
        UUID changeId = com.bazaarvoice.emodb.common.uuid.TimeUUIDs.uuidForTimestamp(now);
        store.update(TABLE, KEY1, changeId, Deltas.mapBuilder().put("key", "value0").build(), audit, WriteConsistency.STRONG);
        record = store.get(TABLE, KEY1);

        // Record just created; all three timestamps should equal the initial timestamp
        Date firstUpdateDate = now;
        Date lastMutateDate = firstUpdateDate;
        Date lastUpdateDate = firstUpdateDate;
        verifyContentAndTimestamps(record, "value0", firstUpdateDate, lastMutateDate, lastUpdateDate);

        // Make a non-mutative change
        now = new Date(now.getTime() + 1000);
        changeId = com.bazaarvoice.emodb.common.uuid.TimeUUIDs.uuidForTimestamp(now);
        store.update(TABLE, KEY1, changeId, Deltas.noop(), audit, WriteConsistency.STRONG);
        record = store.get(TABLE, KEY1);
        // Only the last update date should have changed
        lastUpdateDate = now;
        verifyContentAndTimestamps(record, "value0", firstUpdateDate, lastMutateDate, lastUpdateDate);

        // Make a mutative change
        now = new Date(now.getTime() + 1000);
        changeId = com.bazaarvoice.emodb.common.uuid.TimeUUIDs.uuidForTimestamp(now);
        store.update(TABLE, KEY1, changeId, Deltas.mapBuilder().put("key", "value1").build(), audit, WriteConsistency.STRONG);
        record = store.get(TABLE, KEY1);
        // Both last update and mutate dates should have changed
        lastMutateDate = lastUpdateDate = now;
        verifyContentAndTimestamps(record, "value1", firstUpdateDate, lastMutateDate, lastUpdateDate);

        // Compact the record twice; once to create the compaction record and once again to delete deltas
        store.compact(TABLE, KEY1, Duration.ZERO, ReadConsistency.STRONG, WriteConsistency.STRONG);
        SystemClock.tick();
        store.compact(TABLE, KEY1, Duration.ZERO, ReadConsistency.STRONG, WriteConsistency.STRONG);
        // Verify that we have no deltas and one compaction with compacted delta
        assertEquals(getDeltas(store.getTimeline(TABLE, KEY1, true, false, null, null, false, 100, ReadConsistency.STRONG)).size(), 0);
        assertEquals(getCompactions(store.getTimeline(TABLE, KEY1, true, false, null, null, false, 100, ReadConsistency.STRONG)).size(), 1);

        // Don't change the record, but verify that the old values are still present
        record = store.get(TABLE, KEY1);
        verifyContentAndTimestamps(record, "value1", firstUpdateDate, lastMutateDate, lastUpdateDate);

        // Repeat the compaction process, but this time the most recent delta is non-mutative
        now = new Date(now.getTime() + 1000);
        changeId = com.bazaarvoice.emodb.common.uuid.TimeUUIDs.uuidForTimestamp(now);
        store.update(TABLE, KEY1, changeId, Deltas.noop(), audit, WriteConsistency.STRONG);
        store.compact(TABLE, KEY1, Duration.ZERO, ReadConsistency.STRONG, WriteConsistency.STRONG);
        SystemClock.tick();
        store.compact(TABLE, KEY1, Duration.ZERO, ReadConsistency.STRONG, WriteConsistency.STRONG);
        // Verify that we have no deltas and one compaction with compacted delta
        assertEquals(getDeltas(store.getTimeline(TABLE, KEY1, true, false, null, null, false, 100, ReadConsistency.STRONG)).size(), 0);
        assertEquals(getCompactions(store.getTimeline(TABLE, KEY1, true, false, null, null, false, 100, ReadConsistency.STRONG)).size(), 1);
        // Only last update date should have changed
        record = store.get(TABLE, KEY1);
        lastUpdateDate = now;
        verifyContentAndTimestamps(record, "value1", firstUpdateDate, lastMutateDate, lastUpdateDate);

        // Make a non-mutative change
        now = new Date(now.getTime() + 1000);
        changeId = com.bazaarvoice.emodb.common.uuid.TimeUUIDs.uuidForTimestamp(now);
        store.update(TABLE, KEY1, changeId, Deltas.noop(), audit, WriteConsistency.STRONG);
        record = store.get(TABLE, KEY1);
        // Only the last update date should have changed
        lastUpdateDate = now;
        verifyContentAndTimestamps(record, "value1", firstUpdateDate, lastMutateDate, lastUpdateDate);

        // Make a mutative change
        now = new Date(now.getTime() + 1000);
        changeId = com.bazaarvoice.emodb.common.uuid.TimeUUIDs.uuidForTimestamp(now);
        store.update(TABLE, KEY1, changeId, Deltas.mapBuilder().put("key", "value2").build(), audit, WriteConsistency.STRONG);
        record = store.get(TABLE, KEY1);
        // Both last update and mutate dates should have changed
        lastMutateDate = lastUpdateDate = now;
        verifyContentAndTimestamps(record, "value2", firstUpdateDate, lastMutateDate, lastUpdateDate);
    }

    @Test
    public void testRecordTimestampsWithEventTags() throws Exception {
        DataStore store = new InMemoryDataStore(new MetricRegistry());

        TableOptions options = new TableOptionsBuilder().setPlacement("default").build();
        assertFalse(store.getTableExists(TABLE));
        store.createTable(TABLE, options, Collections.<String,Object >emptyMap(), newAudit("create table"));
        assertTrue(store.getTableExists(TABLE));

        // Record does not exist initially, so all dates should not be present
        Map<String, Object> record = store.get(TABLE, KEY1);
        assertTrue(Intrinsic.isDeleted(record));
        assertNull(Intrinsic.getFirstUpdateAt(record));
        assertNull(Intrinsic.getLastUpdateAt(record));
        assertNull(Intrinsic.getLastMutateAt(record));

        Audit audit = newAudit("test");

        Date now = new Date(System.currentTimeMillis() - 5000);
        // Normally using uuidForTimestamp is a bad idea, but for this test we need precise control over the UUID timestamps.
        // So long as no two are created with the same time we'll be ok.
        UUID changeId = com.bazaarvoice.emodb.common.uuid.TimeUUIDs.uuidForTimestamp(now);
        store.update(TABLE, KEY1, changeId, Deltas.mapBuilder().put("key", "value0").build(), audit, WriteConsistency.STRONG);
        Date firstUpdateDate = now;
        // Perform a mutative update
        now = new Date(now.getTime() + 1000);
        // This will be the last time the content is mutated in this test; save the date
        Date lastContentMutateDate = now;
        changeId = com.bazaarvoice.emodb.common.uuid.TimeUUIDs.uuidForTimestamp(now);
        store.update(TABLE, KEY1, changeId, Deltas.mapBuilder().put("key", "value1").build(), audit, WriteConsistency.STRONG);
        // Perform a non-mutative update
        now = new Date(now.getTime() + 1000);
        changeId = com.bazaarvoice.emodb.common.uuid.TimeUUIDs.uuidForTimestamp(now);
        store.update(TABLE, KEY1, changeId, Deltas.noop(), audit, WriteConsistency.STRONG);
        // Perform another non-mutative update, this time with different event tags
        now = new Date(now.getTime() + 1000);
        // This will be the last time the content or tags are mutated in this test; save the date
        Date lastMutateDate = now;
        changeId = com.bazaarvoice.emodb.common.uuid.TimeUUIDs.uuidForTimestamp(now);
        store.updateAll(
                ImmutableList.of(new Update(TABLE, KEY1, changeId, Deltas.noop(), audit, WriteConsistency.STRONG)),
                ImmutableSet.of("tag1"));
        // Perform a final non-mutative update with the same event tags
        now = new Date(now.getTime() + 1000);
        changeId = com.bazaarvoice.emodb.common.uuid.TimeUUIDs.uuidForTimestamp(now);
        store.updateAll(
                ImmutableList.of(new Update(TABLE, KEY1, changeId, Deltas.noop(), audit, WriteConsistency.STRONG)),
                ImmutableSet.of("tag1"));
        Date lastUpdateDate = now;

        // Verify the content and timestamps
        record = store.get(TABLE, KEY1);
        verifyContentAndTimestamps(record, "value1", firstUpdateDate, lastContentMutateDate, lastUpdateDate);

        // Perform a compaction
        store.compact(TABLE, KEY1, Duration.ZERO, ReadConsistency.STRONG, WriteConsistency.STRONG);
        // Get the compaction
        List<Compaction> compactions = getCompactions(store.getTimeline(TABLE, KEY1, true, false, null, null, false, 100, ReadConsistency.STRONG));
        assertEquals(compactions.size(), 1);
        Compaction compaction = compactions.get(0);
        // Verify the timestamps within the compaction
        assertEquals(TimeUUIDs.getTimeMillis(compaction.getFirst()), firstUpdateDate.getTime());
        assertEquals(TimeUUIDs.getTimeMillis(compaction.getCutoff()), lastUpdateDate.getTime());
        assertEquals(TimeUUIDs.getTimeMillis(compaction.getLastMutation()), lastMutateDate.getTime());
        assertEquals(TimeUUIDs.getTimeMillis(compaction.getLastContentMutation()), lastContentMutateDate.getTime());

        // Re-verify the content and timestamps
        record = store.get(TABLE, KEY1);
        verifyContentAndTimestamps(record, "value1", firstUpdateDate, lastContentMutateDate, lastUpdateDate);
    }

    private void verifyContentAndTimestamps(Map<String, Object> record, String expectedValue, Date expectedFirstUpdateDate,
                                            Date expectedLastMutateDate, Date expectedLastUpdateDate) {
        assertEquals(record.get("key"), expectedValue);
        if (expectedFirstUpdateDate == null) {
            assertNull(Intrinsic.getFirstUpdateAt(record));
        } else {
            assertEquals(Intrinsic.getFirstUpdateAt(record), expectedFirstUpdateDate);
        }
        if (expectedLastMutateDate == null) {
            assertNull(Intrinsic.getLastMutateAt(record));
        } else {
            assertEquals(Intrinsic.getLastMutateAt(record), expectedLastMutateDate);
        }
        if (expectedLastUpdateDate == null) {
            assertNull(Intrinsic.getLastUpdateAt(record));
        } else {
            assertEquals(Intrinsic.getLastUpdateAt(record), expectedLastUpdateDate);
        }
    }

    private Audit newAudit(String comment) {
        return new AuditBuilder().
                setProgram("test").
                setUser("root").
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

    private List<Audit> getAudits(Iterator<Change> changeIter) {
        List<Audit> audits = Lists.newArrayList();
        while (changeIter.hasNext()) {
            Change change = changeIter.next();
            if (change.getAudit() != null) {
                audits.add(change.getAudit());
            }
        }
        return audits;
    }

}
