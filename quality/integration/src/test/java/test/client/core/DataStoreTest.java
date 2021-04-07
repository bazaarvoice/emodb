package test.client.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.FacadeOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.Table;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.client.DataStoreStreaming;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;
import test.client.commons.TestModuleFactory;
import test.client.commons.utils.DataStoreHelper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static test.client.commons.utils.DataStoreHelper.compactdata;
import static test.client.commons.utils.DataStoreHelper.createDataTable;
import static test.client.commons.utils.DataStoreHelper.getTimelineChangeList;
import static test.client.commons.utils.RetryUtils.snooze;
import static test.client.commons.utils.TableUtils.getAudit;
import static test.client.commons.utils.TableUtils.getTemplate;

@Test(timeOut = 360000)
@Guice(moduleFactory = TestModuleFactory.class)
public class DataStoreTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataStoreTest.class);

    private static final String NON_EXISTENT_DOC_SIGNATURE = "00000000000000000000000000000000";

    @Inject
    @Named("placement")
    private String placement;

    @Inject
    @Named("remotePlacement")
    private String remotePlacement;

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
            LOGGER.info("Deleting table: {}", name);
            try {
                dataStore.dropTable(name, getAudit("drop table " + name));
            } catch (Exception e) {
                LOGGER.warn("Error to delete table", e);
            }
        });
    }

    @Test
    public void testTableOptions() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "table_option", "table_option", runID);
        String tableName = createTable(helper);

        TableOptions actualTableOptions = dataStore.getTableOptions(tableName);
        TableOptions expectedTableOptions = new TableOptionsBuilder().setPlacement(placement).build();

        assertEquals(actualTableOptions, expectedTableOptions, "Did not get expected table options");
        assertEquals(actualTableOptions.getPlacement(), expectedTableOptions.getPlacement(),
                "Did not get expected placement in TableOptions");
    }

    @Test
    public void testStreamingUpdate() {
        final int documentCount = 300;
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "streaming_content", "streaming_update_all", runID);
        String tableName = createTable(helper);

        List<Update> updates = helper.generateUpdatesList(documentCount);
        List<String> updateIDs = updates.stream().map(Update::getKey).collect(Collectors.toList());
        dataStore.updateAll(updates);

        long tableSize = dataStore.getTableApproximateSize(tableName, documentCount * 2);

        assertEquals(tableSize, documentCount, "Did not find right amount of documents");
        updateIDs.forEach(key -> assertNotNull(dataStore.get(tableName, key)));
    }

    @Test
    public void testScan() {
        final int documentCount = 39;
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "scan", "scan", runID);
        String tableName = createTable(helper);

        List<Update> updateList = helper.generateUpdatesList(documentCount);
        // Easier to compare the IDs than Update.
        Set<String> expectedIDs = updateList.stream()
                .map(Update::getKey)
                .collect(Collectors.toSet());
        List<String> actualIDs = new ArrayList<>();

        // Update all the documents at once.
        DataStoreStreaming.updateAll(dataStore, updateList);

        //Sanity checking documents
        int middle = documentCount / 2;
        String documentToCheck = String.format("%s-%s", helper.getDocumentKey(), middle);
        Map<String, Object> actualDocument = dataStore.get(tableName, documentToCheck);
        assertEquals(Integer.parseInt(actualDocument.get("iteration").toString()), middle,
                "Attribute value mismatch for selected content.");
        assertFalse(Intrinsic.isDeleted(actualDocument), "Document should not be deleted!");

        int limit = 7;
        int count;
        String fromID = null;
        do {
            dataStore.scan(tableName, fromID, limit, false, ReadConsistency.STRONG).forEachRemaining(doc -> actualIDs.add(Intrinsic.getId(doc)));
            fromID = Iterables.getLast(actualIDs); // Get ID from where to scan again.
            count = (actualIDs.size() % limit) + limit; // Count how many items were returned
        } while (count == limit);

        assertEquals(actualIDs.size(), documentCount, "Not all ids returned from scan");
        assertEquals(new HashSet<>(actualIDs), expectedIDs,
                String.format("dataStore.scan did not return all expected IDs for table %s", tableName));
    }

    @Test(expectedExceptions = TableExistsException.class)
    public void testTableExistsException() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "table_exists", "table_exists", runID);
        String tableName = createTable(helper);

        assertTrue(dataStore.getTableExists(tableName));

        // This should throw an exception expected by the test.
        createDataTable(dataStore, tableName, ImmutableMap.of("type", "table_2"), placement);
    }

    @Test
    public void testGetTablePlacements() {
        Collection<String> placements = dataStore.getTablePlacements();
        assertTrue(placements.contains("app_global:default"));
        assertTrue(placements.contains("ugc_global:ugc"));
        assertTrue(placements.contains("catalog_global:cat"));
    }

    @Test
    public void testGetTableTemplate() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "get_table_template", "get_table_template", runID);
        String tableName = createTable(helper);

        assertEquals(dataStore.getTableTemplate(tableName), helper.getTemplate());

        Map<String, String> newTemplate = ImmutableMap.of("new", "template");
        dataStore.setTableTemplate(tableName, newTemplate, getAudit(String.format("Update template for %s", tableName)));

        assertEquals(dataStore.getTableTemplate(tableName), newTemplate, "Template was not updated");
        assertNotEquals(dataStore.getTableTemplate(tableName), helper.getTemplate(), "Template was not updated");
    }

    @Test
    public void testListTables() {
        final int tableCount = 3;
        // Create at least enough tables to pass the test. In case there are not enought tables.
        for (int i = 1; i <= tableCount; i++) {
            final String tableName = String.format("test_list_table_%s_%s", i, runID);
            createDataTable(dataStore, tableName,
                    getTemplate("list_table", "dataStoreClient", runID), placement);
            tablesToCleanupAfterTest.add(tableName);
        }
        List<Table> tableList = Lists.newArrayList(dataStore.listTables(null, 3));
        assertEquals(tableList.size(), tableCount, "List tables did not return enough tables");
    }

    @Test
    public void testDelete() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "delete", "delete", runID);
        String tableName = createTable(helper);

        final int totalDocs = 5;

        List<Update> updates = helper.generateUpdatesList(totalDocs);
        dataStore.updateAll(updates);

        int docNumToDelete = random.nextInt(totalDocs);
        Update updateTodelete = updates.get(docNumToDelete);

        // Delete Record
        dataStore.update(tableName, updateTodelete.getKey(), TimeUUIDs.newUUID(), Deltas.delete(),
                getAudit("Delete Update"), WriteConsistency.STRONG);

        // Check its deleted
        Map<String, Object> deletedDocument = dataStore.get(tableName, updateTodelete.getKey());
        assertTrue(Intrinsic.isDeleted(deletedDocument), "Document was not deleted.");

        // Make sure the deleted document is not returned when scanning.
        dataStore.scan(tableName, null, totalDocs, false, ReadConsistency.STRONG).forEachRemaining(doc -> assertNotEquals(Intrinsic.getId(doc), updateTodelete.getKey()));
    }

    @Test
    public void testGetTimeline() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "timeline", "get_timeline", runID);
        String tableName = createTable(helper);

        final int totalDocs = 3;

        List<Update> updates = helper.generateUpdatesList(totalDocs);

        // Create new updates to couple of documents and add them to list to keep track of them
        Update doc1_update1 = new Update(tableName, updates.get(1).getKey(), TimeUUIDs.newUUID(),
                Deltas.mapBuilder().put("update", 1).build(),
                getAudit("get_timeline_doc1_update1"));
        updates.add(doc1_update1);

        Update doc2_update1 = new Update(tableName, updates.get(2).getKey(), TimeUUIDs.newUUID(),
                Deltas.mapBuilder().put("update", 1).build(),
                getAudit("get_timeline_doc2_update1"));
        updates.add(doc2_update1);

        Update doc2_update2 = new Update(tableName, updates.get(2).getKey(), TimeUUIDs.newUUID(),
                Deltas.mapBuilder().put("update", 2).build(),
                getAudit("get_timeline_doc2_update2"));
        updates.add(doc2_update2);

        dataStore.updateAll(updates);

        // Quick read
        assertEquals(dataStore.get(tableName, doc1_update1.getKey(), ReadConsistency.STRONG).get("update"), 1);
        assertEquals(dataStore.get(tableName, doc2_update2.getKey(), ReadConsistency.STRONG).get("update"), 2);

        List<List<Change>> timelineList = updates.stream()
                .map(Update::getKey)
                .distinct()
                .sorted()
                .map(documentID -> getTimelineChangeList(dataStore, tableName, documentID, true, false))
                .collect(Collectors.toList());

        // Get timeline for each document

        // Check timeline
        assertEquals(timelineList.size(), totalDocs, "Got more timelines than docs created.");
        for (int i = 0; i < totalDocs; i++) {
            List<Change> timeline = timelineList.get(i);
            assertEquals(timeline.size(), i + 1, "Unexpected number of deltas found!");
        }
        // Check timeline-2's last delta in timeline
        assertEquals(timelineList.get(1).get(1).getDelta(), doc1_update1.getDelta(), "Timeline Delta mismatch");
        // Check timeline-3's middle delta in timeline
        assertEquals(timelineList.get(2).get(1).getDelta(), doc2_update1.getDelta(), "Timeline Delta mismatch");
        // Check timeline-3's last delta in timeline
        assertEquals(timelineList.get(2).get(2).getDelta(), doc2_update2.getDelta(), "Timeline Delta mismatch");
    }

    @DataProvider
    public Object[][] dataprovider_testSplit() {
        return new Object[][]{
                {123, 50},
                {333, 7},
                {31, 222},
        };
    }

    @Test(dataProvider = "dataprovider_testSplit")
    public void testSplit(final int splitSize, final int limit) {
        final int totalDocs = 500 + random.nextInt(2501); // 500 - 3000 documents

        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, Integer.toString(splitSize), "split", runID);
        String tableName = createTable(helper);

        List<Update> expectedUpdates = helper.generateUpdatesList(totalDocs);
        dataStore.updateAll(expectedUpdates);

        // Easier to compare List<String> later on
        Set<String> expectedIDs = expectedUpdates.stream()
                .map(Update::getKey)
                .collect(Collectors.toSet());
        Set<String> actualIDs = new HashSet<>();

        Set<Delta> actualDeltas = new HashSet<>();
        dataStore.getSplits(tableName, splitSize).forEach(split -> {
            int[] count = new int[1];
            String[] from = {null};
            do {
                count[0] = 0;
                dataStore.getSplit(tableName, split, from[0], limit, false, ReadConsistency.STRONG)
                        .forEachRemaining((Map<String, Object> document) -> {
                            count[0] = count[0] + 1;
                            actualIDs.add(Intrinsic.getId(document));
                            actualDeltas.add(Deltas.literal(
                                    document.entrySet().stream()
                                            .filter(doc -> doc.getKey().equalsIgnoreCase("iteration"))
                                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
                            from[0] = Intrinsic.getId(document);
                        });
            } while (count[0] == limit);
        });

        assertEquals(actualIDs.size(), totalDocs);
        assertEquals(actualIDs, expectedIDs, "Got unexpected IDs!");
        final Set<Delta> expected = expectedUpdates
                .stream()
                .map(Update::getDelta)
                .collect(Collectors.toSet());
        assertEquals(actualDeltas, expected);
    }

    @Test()
    public void testCompact() {
        final int totalIterations = 3;
        final int timelineSize = totalIterations + 1; //Count for compaction delta.
        final Delta expectedDelta = Deltas.mapBuilder().put("iteration", totalIterations).build();
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, Integer.toString(totalIterations), "compact", runID);
        String tableName = createTable(helper);

        List<Update> documentUpdates = helper.updateDocumentMultipleTimes(totalIterations, 1);

        snooze(2);
        compactdata(dataStore, tableName, helper.getDocumentKey(), Duration.ofSeconds(1));
        snooze(10);
        Compaction originalCompaction = checkCompaction(tableName, helper.getDocumentKey(), documentUpdates,
                totalIterations, timelineSize, expectedDelta);

        // Make sure no new compaction records are created without updates between compactions
        snooze(2);
        compactdata(dataStore, tableName, helper.getDocumentKey(), Duration.ofSeconds(1));
        snooze(5);
        Compaction secondCompaction = checkCompaction(tableName, helper.getDocumentKey(), documentUpdates,
                totalIterations, timelineSize, expectedDelta);

        assertEquals(secondCompaction, originalCompaction);
    }

    @Test
    public void testMultipleCompact() {
        final int totalIterations = 3;
        int total_documents_updated = totalIterations;
        int timelineSize = total_documents_updated + 1; //Count for compaction delta.

        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "compact_multiple", "compact", runID);
        String tableName = createTable(helper);

        List<Update> documentsUpdates = helper.updateDocumentMultipleTimes(totalIterations, 1);

        snooze(1);
        compactdata(dataStore, tableName, helper.getDocumentKey(), Duration.ofSeconds(1));
        snooze(5);

        Compaction firstCompaction = checkCompaction(tableName, helper.getDocumentKey(), documentsUpdates,
                total_documents_updated, timelineSize, Deltas.mapBuilder().put("iteration", totalIterations).build());
        documentsUpdates.addAll(helper.updateDocumentMultipleTimes(totalIterations, 4));
        total_documents_updated += totalIterations;
        timelineSize += totalIterations + 1;
        snooze(1);
        compactdata(dataStore, tableName, helper.getDocumentKey(), Duration.ofSeconds(1));
        snooze(5);
        Compaction secondCompaction = checkCompaction(tableName, helper.getDocumentKey(), documentsUpdates,
                total_documents_updated, timelineSize, Deltas.mapBuilder().put("iteration", 6).build());

        assertNotEquals(secondCompaction, firstCompaction, "Two compactions should be different");
    }

    @DataProvider
    public Object[][] dataProvider_testMultiGet() {
        DataStoreHelper helper1 = new DataStoreHelper(dataStore, placement, "multi_get_1", "multiget", runID);
        DataStoreHelper helper2 = new DataStoreHelper(dataStore, placement, "multi_get_2", "multiget", runID);

        String tableOne = createTable(helper1);
        String keyOne = helper1.getDocumentKey();

        String tableTwo = createTable(helper2);

        String keyTwo = helper2.getDocumentKey();

        Delta deltaOne = Deltas.mapBuilder().put("base", keyOne).build();
        Delta deltaTwo = Deltas.mapBuilder().put("base", keyTwo).build();

        dataStore.update(tableOne, keyOne, TimeUUIDs.newUUID(), deltaOne, getAudit("Update doc1 with deltaOne"));
        dataStore.update(tableOne, keyOne, TimeUUIDs.newUUID(), deltaTwo, getAudit("Update doc1 with deltaTwo"));
        dataStore.update(tableTwo, keyTwo, TimeUUIDs.newUUID(), deltaTwo, getAudit("Update doc2 with deltaTwo"));
        return new Object[][]{
                {
                        Arrays.asList(Coordinate.of(tableOne, keyOne), Coordinate.of(tableTwo, keyTwo)),
                        new TreeSet<>(Arrays.asList(tableOne, tableTwo)),
                        new TreeSet<>(Arrays.asList(keyOne, keyTwo))
                },
                {
                        Arrays.asList(Coordinate.of(tableOne, keyOne), Coordinate.of(tableOne, keyTwo)),
                        new TreeSet<>(Arrays.asList(tableOne, tableOne)),
                        new TreeSet<>(Arrays.asList(keyOne, keyTwo))
                },
                {
                        Arrays.asList(Coordinate.of(tableOne, keyOne), Coordinate.of(tableOne, keyTwo), Coordinate.of(tableTwo, keyTwo)),
                        new TreeSet<>(Arrays.asList(tableOne, tableOne, tableTwo)),
                        new TreeSet<>(Arrays.asList(keyOne, keyTwo, keyTwo))
                }
        };
    }

    @Test(dataProvider = "dataProvider_testMultiGet")
    public void testMultiGet(List<Coordinate> coordinates, Set<String> expectedTables, Set<String> expectedKeys) {
        Set<String> actualTables = new TreeSet<>();
        Set<String> actualKeys = new TreeSet<>();

        dataStore.multiGet(coordinates).forEachRemaining(document -> {
            actualTables.add(Intrinsic.getTable(document));
            actualKeys.add(Intrinsic.getId(document));
        });

        assertEquals(actualKeys, expectedKeys, "Keys mismatch");
        assertEquals(actualTables, expectedTables, "Tables mismatch");
    }

    @Test
    public void testMultiGet_fakeRecord() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "fake_record", "multi_get", runID);
        final String tableName = createTable(helper);

        List<Map<String, Object>> returnedDocs = Lists.newArrayList(
                dataStore.multiGet(Lists.newArrayList(Coordinate.of(tableName, "fake_record"))));

        assertEquals(returnedDocs.size(), 1, "Should only return one document");
        assertTrue(Intrinsic.isDeleted(returnedDocs.get(0)), "Returned record should be deleted.");
        assertEquals(Intrinsic.getSignature(returnedDocs.get(0)), NON_EXISTENT_DOC_SIGNATURE, "Signature should be 0");
        assertEquals(Intrinsic.getVersion(returnedDocs.get(0)), new Long(0), "Version should be 0");
        assertEquals(Intrinsic.getId(returnedDocs.get(0)), "fake_record", "Wrong key returned");
        assertEquals(Intrinsic.getTable(returnedDocs.get(0)), tableName, "Wrong table returned");
    }

    @Test
    public void testMultiGet_deletedRecord() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "deleted_record", "multi_get", runID);
        String tableName = createTable(helper);

        dataStore.update(
                tableName,
                helper.getDocumentKey(),
                TimeUUIDs.newUUID(),
                Deltas.literal(ImmutableMap.of("base", helper.getDocumentKey())),
                getAudit("Adding document"));

        dataStore.update(
                tableName,
                helper.getDocumentKey(),
                TimeUUIDs.newUUID(),
                Deltas.delete(),
                getAudit("Delete record"));
        List<Map<String, Object>> returnedDocs = Lists.newArrayList(
                dataStore.multiGet(Lists.newArrayList(Coordinate.of(tableName, helper.getDocumentKey()))));

        assertEquals(returnedDocs.size(), 1, "Should only return one document");
        assertTrue(Intrinsic.isDeleted(returnedDocs.get(0)), "Returned record should be deleted.");
        assertNotEquals(Intrinsic.getSignature(returnedDocs.get(0)), NON_EXISTENT_DOC_SIGNATURE, "Signature should not be 0");
        assertNotEquals(Intrinsic.getVersion(returnedDocs.get(0)), 0L, "Version should not be 0");
        assertEquals(Intrinsic.getId(returnedDocs.get(0)), helper.getDocumentKey(), "Wrong key returned");
        assertEquals(Intrinsic.getTable(returnedDocs.get(0)), tableName, "Wrong table returned");
    }

    @Test
    public void testMultiGet_placements() {
        DataStoreHelper defaultPlacementhelper = new DataStoreHelper(dataStore, placement, "default_placement", "multi_get", runID);
        DataStoreHelper diffPlacementHelper = new DataStoreHelper(dataStore, remotePlacement, "different_placement", "multi_get", runID);

        String defaultPlacementTableName = createTable(defaultPlacementhelper);

        String diffPlacementTableName = createTable(diffPlacementHelper);

        List<Coordinate> coordinates = Lists.newArrayList(
                Coordinate.of(defaultPlacementTableName, "key"),
                Coordinate.of(diffPlacementTableName, "key")
        );

        try {
            dataStore.multiGet(coordinates);
            fail("UnknownPlacementException not thrown");
        } catch (UnknownPlacementException e) {
            assertEquals(e.getPlacement(), remotePlacement, "Wrong placement threw exception");
            assertEquals(e.getTable(), diffPlacementTableName, "Wrong table in exception");
        }
    }

    @Test
    public void testFacade() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, remotePlacement, "facade", "facade", runID);
        final String tableName = createTable(helper);

        dataStore.createFacade(tableName, new FacadeOptionsBuilder().setPlacement(placement).build(),
                getAudit(String.format("Create facade of '%s'", tableName)));

        assertTrue(dataStore.getTableMetadata(tableName).getAvailability().isFacade());
        assertEquals(dataStore.getTableMetadata(tableName).getAvailability().getPlacement(), placement);

        // Make sure Facade update happen successfully
        updateFacadeTable(tableName, helper.getDocumentKey(), "{\"update\":1}");

        // Make sure update is read out properly
        assertEquals(
                dataStore.get(tableName, helper.getDocumentKey()).get("update").toString(),
                "1",
                "Proper update did not occur!");
    }

    @Test(expectedExceptions = UnknownTableException.class,
    expectedExceptionsMessageRegExp = ".*Unknown table: gatekeeper_datastore_client:facade_drop_master_.*")
    public void testFacade_negative_dropMaster() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, remotePlacement, "drop_master", "facade", runID);
        String tableName = createTable(helper);

        dataStore.createFacade(tableName, new FacadeOptionsBuilder().setPlacement(placement).build(),
                getAudit("Create facade table for dropMaster"));

        dataStore.dropTable(tableName, getAudit("Drop master table"));
        assertFalse(dataStore.getTableExists(tableName), "Table should be deleted");

        updateFacadeTable(tableName, "failed_update", "{\"update\":1}");
        fail("Facade update should not be successful!");
    }

    @Test
    public void testFacade_dropFacade_is_not_supported_by_client() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, remotePlacement, "drop_facade", "facade", runID);
        String tableName = createTable(helper);

        dataStore.createFacade(tableName, new FacadeOptionsBuilder().setPlacement(placement).build(),
                getAudit("Create facade table for dropFacade"));

        try {
            updateFacadeTable(tableName, "passed_update", "{\"update\":1}");
        } catch (Exception e) {
            LOGGER.error("Error: ", e);
            fail("Something went wrong with updating facade");
        }

        assertEquals(dataStore.get(tableName, "passed_update").get("update").toString(),
                "1",
                "Proper table update did not occur!");

        Audit deleteAudit = getAudit(String.format("Delete facade table: %s", tableName));

        try {
            dataStore.dropFacade(tableName, placement, deleteAudit);
            fail("Drop facade shouldn't be successful");
        } catch (Exception e) {
            LOGGER.error("Exception: ", e);
            assertTrue(e instanceof UnsupportedOperationException, "Proper exception not thrown");
        }
    }

    @Test(expectedExceptions = UnknownTableException.class,
            expectedExceptionsMessageRegExp = ".*Unknown table: gatekeeper_datastore_client:facade_dne_master_table_.*")
    public void testFacade_negative_DNEMasterTable() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "dne_master_table", "facade", runID);
        assertFalse(dataStore.getTableExists(helper.getTableName()), "Table should not exists!");

        dataStore.createFacade(helper.getTableName(), new FacadeOptionsBuilder().setPlacement(placement).build(),
                getAudit("DNE Master Facade table"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class,
            expectedExceptionsMessageRegExp = ".*Cannot create a facade in the same placement as its table.*")
    public void testFacade_negative_samePlacementFacade() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "facade_same_placement", "facade", runID);
        String tableName = createTable(helper);

        dataStore.createFacade(tableName, new FacadeOptionsBuilder().setPlacement(placement).build(),
                getAudit("same placement create facade"));
    }

    @Test
    public void testDropTable() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "drop_table", "drop_table", runID);
        final String tableName = createTable(helper);

        dataStore.dropTable(tableName, getAudit("drop table"));
        assertFalse(dataStore.getTableExists(tableName), "Table should be deleted");
        tablesToCleanupAfterTest.remove(tableName);
    }

    @Test(expectedExceptions = UnknownTableException.class,
            expectedExceptionsMessageRegExp = ".*Unknown table: gatekeeper_datastore_client:drop_table_fake_table.*")
    public void testDropTable_negative_fakeTable() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "fake_table", "drop_table", runID);
        dataStore.dropTable(helper.getTableName(), getAudit("drop fake table"));
    }

    private String createTable(DataStoreHelper helper) {
        String tableName = helper.createTable();
        tablesToCleanupAfterTest.add(tableName);
        return tableName;
    }

    private void updateFacadeTable(String tableName, String document, String update) {
        Audit audit = getAudit(String.format("Update facade table/document %s/%s", tableName, document));
        List<Update> updates = ImmutableList.of(
                new Update(tableName, document, TimeUUIDs.newUUID(), Deltas.fromString(update), audit)
        );
        dataStore.updateAllForFacade(updates);
    }

    private Compaction checkCompaction(String tableName, String key, List<Update> updates, int expectedDocsInCompaction,
                                         int timelineSize, Delta expectedDelta) {
        List<Change> timeline = getTimelineChangeList(
                dataStore, tableName, key,
                true, false
        );
        assertEquals(timeline.size(), timelineSize, "Incorrect timelineSize");

        List<Compaction> compactions = new ArrayList<>();
        timeline.stream().filter(change -> change.getCompaction() != null).sorted(Comparator.comparing(Change::getTimestamp))
                .forEach(change -> compactions.add(change.getCompaction()));

        Compaction actualCompaction = Iterables.getLast(compactions);

        assertEquals(actualCompaction.getCompactedDelta().toString(), expectedDelta.toString().replace("..,", ""),
                "Delta Mismatch");
        assertEquals(actualCompaction.getCount(), expectedDocsInCompaction,
                "Unexpected number of docs in compaction");
        assertEquals(actualCompaction.getFirst(), updates.get(0).getChangeId(),
                "First document UUID in compaction was unexpected.");
        assertEquals(actualCompaction.getCutoff(), Iterables.getLast(updates).getChangeId(),
                "Last document UUID in compaction was unexpected.");

        return actualCompaction;
    }
}
