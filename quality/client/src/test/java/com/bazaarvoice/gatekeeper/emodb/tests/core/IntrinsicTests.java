package com.bazaarvoice.gatekeeper.emodb.tests.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.gatekeeper.emodb.commons.TestModuleFactory;
import com.bazaarvoice.gatekeeper.emodb.commons.utils.DataStoreHelper;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.bazaarvoice.gatekeeper.emodb.commons.utils.DataStoreHelper.compactdata;
import static com.bazaarvoice.gatekeeper.emodb.commons.utils.DataStoreHelper.generateUpdatesList;
import static com.bazaarvoice.gatekeeper.emodb.commons.utils.DataStoreHelper.getTimelineChangeList;
import static com.bazaarvoice.gatekeeper.emodb.commons.utils.DataStoreHelper.updateDocument;
import static com.bazaarvoice.gatekeeper.emodb.commons.utils.RetryUtils.snooze;
import static com.bazaarvoice.gatekeeper.emodb.commons.utils.TableUtils.getAudit;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(groups = {"emodb.core.all", "emodb.core.intrinsic", "intrinsic"}, timeOut = 36000)
@Guice(moduleFactory = TestModuleFactory.class)
public class IntrinsicTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(IntrinsicTests.class);

    private static final String NON_EXISTENT_DOC_SIGNATURE = "00000000000000000000000000000000";

    @Inject
    @Named("placement")
    private String placement;

    @Inject
    @Named("runID")
    private String runID;

    @Inject
    private DataStore dataStore;

    private final Random random = new Random();
    private Set<String> tablesToCleanupAfterTest;

    @BeforeTest(alwaysRun = true)
    public void beforeTest() {
        tablesToCleanupAfterTest = new HashSet<>();
    }

    @AfterTest(alwaysRun = true)
    public void afterTest() {
        tablesToCleanupAfterTest.forEach((name) -> {
            LOGGER.info("Deleting blob table: {}", name);
            try {
                dataStore.dropTable(name, getAudit("drop table " + name));
            } catch (Exception e) {
                LOGGER.warn("Error to delete blob table", e);
            }
        });
    }

    @Test
    public void testLateMutateAt_nonMutativeUpdate() {
        final int totalUpdates = 10;

        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "non_mutative_update", "last_mutate_at", runID);
        final String tableName = helper.createTable();
        tablesToCleanupAfterTest.add(tableName);

        // Original updates
        List<Update> originalUpdates = generateUpdatesList(totalUpdates, tableName,
                helper.getDocumentKey());
        dataStore.updateAll(originalUpdates);

        // Update Again
        List<Update> newUpdates = generateUpdatesList(totalUpdates, tableName,
                helper.getDocumentKey());
        final List<String> actual = originalUpdates.stream().map(Update::getKey).collect(Collectors.toList());
        final List<String> expected = newUpdates.stream().map(Update::getKey).collect(Collectors.toList());
        assertEquals(actual, expected, "Old and new Updates should be the same");
        dataStore.updateAll(newUpdates);

        assertEquals(dataStore.getTableApproximateSize(tableName, 100), totalUpdates,
                "More documents found than expected!");
        originalUpdates.forEach(update -> {
            Map<String, Object> document = dataStore.get(tableName, update.getKey());
            assertEquals(Intrinsic.getLastMutateAt(document), new Date(TimeUUIDs.getTimeMillis(update.getChangeId())),
                    "LastMutateAt time changed with 2nd update");
            assertNotEquals(Intrinsic.getLastUpdateAt(document), new Date(TimeUUIDs.getTimeMillis(update.getChangeId())),
                    "LastUpdateAt should be different");
            assertEquals(Intrinsic.getFirstUpdateAt(document), new Date(TimeUUIDs.getTimeMillis(update.getChangeId())),
                    "FirstUpdateAt should be different");
        });
    }

    @Test
    public void testLastMutateAt_deleted() {
        UUID expectedUUID = TimeUUIDs.newUUID();
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "deleted_update", "last_mutate_at", runID);
        final String tableName = helper.createTable();
        tablesToCleanupAfterTest.add(tableName);

        assertEquals(Intrinsic.getSignature(dataStore.get(tableName, helper.getDocumentKey())), NON_EXISTENT_DOC_SIGNATURE);
        assertNull(Intrinsic.getLastMutateAt(dataStore.get(tableName, helper.getDocumentKey())),
                "LastMutateAt should be null before update");

        dataStore.update(tableName, helper.getDocumentKey(), expectedUUID, Deltas.delete(),
                getAudit("delete record only"));

        Map<String, Object> docReturned = dataStore.get(tableName, helper.getDocumentKey());

        assertNotEquals(Intrinsic.getSignature(docReturned), NON_EXISTENT_DOC_SIGNATURE);
        assertEquals(Intrinsic.getLastMutateAt(docReturned), new Date(TimeUUIDs.getTimeMillis(expectedUUID)));
        assertEquals(Intrinsic.getVersion(docReturned), new Long(1));
        assertTrue(Intrinsic.isDeleted(docReturned), "Document should be deleted!");
    }

    @Test
    public void testLastMutateAt_mutativeUpdate() {
        final int totalUpdats = 10;
        final int totalMutatedDocs = 3;

        final UUID originalUUID = TimeUUIDs.newUUID();

        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "mutated_updates", "last_mutate_at", runID);
        final String tableName = helper.createTable();
        tablesToCleanupAfterTest.add(tableName);

        List<Update> originalUpdates = generateUpdatesList(totalUpdats, tableName, helper.getDocumentKey(), originalUUID);
        dataStore.updateAll(originalUpdates);

        Map<String, UUID> updatedKeyUUIDMap = new LinkedHashMap<>();

        for (int i = 0; i < totalMutatedDocs; i++) {
            int docIndex = random.nextInt(totalMutatedDocs);
            String key = originalUpdates.get(docIndex).getKey();
            UUID uuid = TimeUUIDs.newUUID();

            updatedKeyUUIDMap.put(key, uuid);
            dataStore.update(tableName, key, uuid, Deltas.mapBuilder().put("updatedRecord", i).build(),
                    getAudit("Updating record"));
        }

        dataStore.scan(tableName, null, 100, false, ReadConsistency.STRONG).forEachRemaining(doc -> {
            String key = Intrinsic.getId(doc);
            UUID uuid = updatedKeyUUIDMap.getOrDefault(key, originalUUID);
            assertEquals(Intrinsic.getLastMutateAt(doc), new Date(TimeUUIDs.getTimeMillis(uuid)));
        });
    }

    // Resource to be used by all compaction related test
    private DataStoreHelper compactionHelper;
    private UUID compactionUUID;

    @Test
    public void testLastMutateAt_compaction() {
        compactionHelper = new DataStoreHelper(dataStore, placement, "compactions", "last_mutate_at", runID);

        final String tableName = compactionHelper.getTableName();
        if (!dataStore.getTableExists(tableName)) {
            compactionHelper.createTable();
            tablesToCleanupAfterTest.add(tableName);
        }

        int totalUpdates = 10;
        List<Update> updates = compactionHelper.updateDocumentMultipleTimes(totalUpdates, 0);
        compactionUUID = Iterables.getLast(updates).getChangeId();

        List<Change> timeline = getTimelineChangeList(dataStore, tableName,
                compactionHelper.getDocumentKey(), true, false);

        snooze(2); // Making sure that all updates are behind FCL when compacting

        assertEquals(timeline.size(), totalUpdates, "timeline changes should equal total updates issued");

        Change compaction = compactdata(dataStore, tableName,
                compactionHelper.getDocumentKey(), Duration.ofSeconds(2));

        assertEquals(compaction.getCompaction().getCount(), totalUpdates,
                "Total compacted deltas should equal total updates issued");

        timeline = getTimelineChangeList(dataStore, tableName,
                compactionHelper.getDocumentKey(), true, false);
        assertEquals(timeline.size(), totalUpdates + 1,
                "timeline changes should equals total updates issued + 1 compaction record");
        assertEquals(Intrinsic.getLastMutateAt(dataStore.get(tableName, compactionHelper.getDocumentKey())),
                new Date(TimeUUIDs.getTimeMillis(compactionUUID)), "LastMutateAt doesn't match expected");
    }
    
    /*
        This test builds on the last one. Since the last test create a compaction and checks that ~LastMutateAt is not updated,
        add a new record to same table as a non-mutative update and see that nothing changes.
     */

    @Test(dependsOnMethods = {"testLastMutateAt_compaction"})
    public void testLastMutateAt_compactionNonMutated() {
        final String tableName = compactionHelper.getTableName();
        updateDocument(dataStore, tableName, compactionHelper.getDocumentKey(), Deltas.fromString("{..,\"iteration\":9}"));

        assertEquals(Intrinsic.getLastMutateAt(dataStore.get(tableName, compactionHelper.getDocumentKey())),
                new Date(TimeUUIDs.getTimeMillis(compactionUUID)), "LastMutateAt doesn't match expected");
    }
    
    /*
        This test builds on the last one. Since the last test checks that ~LastMutateAt is not updated after a non-mutative delta,
        add a new record to same table as a mutative update and see that ~LastMutateAt is changed.
     */

    @Test(dependsOnMethods = {"testLastMutateAt_compactionNonMutated"})
    public void testLastMutatedAt_compactionMutated() {
        List<Update> updates = compactionHelper.updateDocumentMultipleTimes(1, 11);
        UUID expectedUUID = updates.get(0).getChangeId();

        assertNotEquals(compactionUUID, expectedUUID, "new UUID should not be same as before compaction");
        final String tableName = compactionHelper.getTableName();
        assertEquals(Intrinsic.getLastMutateAt(dataStore.get(tableName, compactionHelper.getDocumentKey())),
                new Date(TimeUUIDs.getTimeMillis(expectedUUID)), "LastMutateAt doesn't match expected");
    }
}
