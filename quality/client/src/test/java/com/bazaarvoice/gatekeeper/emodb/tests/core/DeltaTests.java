package com.bazaarvoice.gatekeeper.emodb.tests.core;

import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.gatekeeper.emodb.commons.TestModuleFactory;
import com.bazaarvoice.gatekeeper.emodb.commons.utils.DataStoreHelper;
import com.google.common.collect.ImmutableMap;
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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.bazaarvoice.gatekeeper.emodb.commons.utils.DataStoreHelper.getTimelineChangeList;
import static com.bazaarvoice.gatekeeper.emodb.commons.utils.DataStoreHelper.updateDocument;
import static com.bazaarvoice.gatekeeper.emodb.commons.utils.TableUtils.getAudit;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(groups = {"emodb.core.all", "emodb.core.delta", "delta"}, timeOut = 36000)
@Guice(moduleFactory = TestModuleFactory.class)
public class DeltaTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeltaTests.class);

    @Inject
    @Named("placement")
    private String placement;

    @Inject
    @Named("runID")
    private String runID;

    @Inject
    private DataStore dataStore;
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
    public void testSmashDelta() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "test_smash_delta", "delta", runID);
        String tableName = createTable(helper);

        String documentKey = helper.getDocumentKey();

        Map<String, String> firstUpdateMap = ImmutableMap.of("field1", "true", "field2", "true");
        Delta firstDelta = Deltas.mapBuilder().putAll(firstUpdateMap).build();
        updateDocument(dataStore, tableName, documentKey, firstDelta);

        // Check first updates
        Map<String, Object> firstReturnedDocument = dataStore.get(tableName, documentKey);
        assertTrue(firstReturnedDocument.keySet().containsAll(firstUpdateMap.keySet()), "Not all keys found");
        firstUpdateMap.keySet().forEach(key -> assertTrue(Boolean.parseBoolean((String) firstReturnedDocument.get(key)), String.format("Value mismatch for %s", key)));

        // Add to it
        helper.updateDocumentMultipleTimes(1, 0);

        // Check it appended
        Map<String, Object> updatedReturnedDocument = dataStore.get(tableName, documentKey);
        assertTrue(updatedReturnedDocument.keySet().containsAll(firstUpdateMap.keySet()), "Not all keys found");
        assertTrue(updatedReturnedDocument.containsKey("iteration"));
        firstUpdateMap.keySet().forEach(key -> assertTrue(Boolean.parseBoolean(firstReturnedDocument.get(key).toString()), String.format("Value mismatch for %s", key)));
        assertEquals(updatedReturnedDocument.get("iteration").toString(), "0");
        // Update with Smash delta
        Delta smashDelta = Deltas.literal(ImmutableMap.of("field3", "true"));
        updateDocument(dataStore, tableName, documentKey, smashDelta);

        // Check old fields are gone
        Map<String, Object> smashUpdateDocument = dataStore.get(tableName, documentKey);
        assertFalse(smashUpdateDocument.keySet().containsAll(firstUpdateMap.keySet()), "Keys from 1st update present");
        assertFalse(smashUpdateDocument.containsKey("iteration"), "Key from 2nd update present");
        assertTrue(smashUpdateDocument.containsKey("field3"), "Smash Delta key not present");
        assertTrue(Boolean.parseBoolean(smashUpdateDocument.get("field3").toString()), "Field value should be true");
    }

    private String createTable(DataStoreHelper helper) {
        String tableName = helper.createTable();
        tablesToCleanupAfterTest.add(tableName);
        return tableName;
    }

    @Test
    public void testDeltasNegative() {
        try {
            Deltas.mapBuilder().put("number", 123).putIfAbsent("number", 4321);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Multiple operations against the same key are not allowed: number"));
        }

        try {
            Deltas.mapBuilder().update("someString", Deltas.setBuilder().addAll("test").remove("test").build()).build();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Multiple operations against the same value are not allowed: test"));
        }

        try {
            Deltas.mapBuilder().update("duplicateString", Deltas.setBuilder().addAll("duplicate", "duplicate").build()).build();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Multiple operations against the same value are not allowed: duplicate"));
        }
    }

    @DataProvider
    public Object[][] dataprovider_testDeltas() {
        Map<String, Object> allThings = ImmutableMap.<String, Object>builder().put("string", "stringEntry").put("number", 2).put("boolean", false).build();
        Map<String, Delta> allThingsDelta = ImmutableMap.<String, Delta>builder()
                .put("string", Deltas.fromString("{\"string\":\"data-team-qa\"}"))
                .put("number", Deltas.fromString("{\"number\":1}"))
                .put("boolean", Deltas.fromString("{\"boolean\":false}")).build();

        Delta mapDelta = Deltas.mapBuilder().put("title", "test_delta_map").put("iteration", 0).build();
        Delta mapDeltaNull = Deltas.mapBuilder().put("title", null).build();
        Delta mapDeltaAbsentConditional = Deltas.mapBuilder().putIfAbsent("number", 4321).build();
        Delta mapDeltaUpdateConditional = Deltas.mapBuilder().updateIfExists("data-team-qa", Deltas.mapBuilder().put("data-team-qa", "Already Exists").build()).build();
        Delta mapDeltaAddAll = Deltas.mapBuilder().putAll(allThings).build();
        Delta mapDeltaUpdateAll = Deltas.mapBuilder().updateAll(allThingsDelta).build();
        Delta mapDeltaRemove = Deltas.mapBuilder().remove("data-team-qa").build();
        Delta mapDeltaRemoveAll = Deltas.mapBuilder().removeAll("data-team-qa", "emodb").build();
        Delta mapDeltaRetain = Deltas.mapBuilder().retain("data-team-qa").build();
        Delta mapDeltaRetainRemove = Deltas.mapBuilder().retain("data-team-qa").remove("emodb").build();
        Delta mapDeltaRetainRemoveRest = Deltas.mapBuilder().retain("data-team-qa").removeRest().build();

        Delta setDeltaStrings = Deltas.mapBuilder().update("strings", Deltas.setBuilder().addAll("data-team-qa", "emodb").build()).build();
        Delta setDeltaNumber = Deltas.mapBuilder().update("number", Deltas.setBuilder().addAll(230).build()).build();
        Delta setDeltaNumbers = Deltas.mapBuilder().update("numbers", Deltas.setBuilder().addAll(7546, 112).build()).build();
        Delta setDeltaStringsNumbers = Deltas.mapBuilder().update("numbers", Deltas.setBuilder().addAll(7546, "data-team-qa", 112, "emodb").build()).build();
        Delta setDeltaAddAll = Deltas.mapBuilder().update("all", Deltas.setBuilder().addAll(7546, "data-team-qa", true, false, null, "emodb").build()).build();
        Delta setDeltaRemove = Deltas.mapBuilder().update("remove", Deltas.setBuilder().remove("data-team-qa").build()).build();
        Delta setDeltaRemoveAll = Deltas.mapBuilder().update("removeAll", Deltas.setBuilder().removeAll("data-team-qa", "emodb").build()).build();
        return new Object[][]{
                {"map_delta_key", mapDelta, "{..,\"iteration\":0,\"title\":\"test_delta_map\"}"},
                {"map_delta_absent_conditional_key", mapDeltaAbsentConditional, "{..,\"number\":if ~ then 4321 end}"},
                {"map_delta_update_conditional_key", mapDeltaUpdateConditional, "{..,\"data-team-qa\":if + then {..,\"data-team-qa\":\"Already Exists\"} end}"},
                {"map_delta_add_all_key", mapDeltaAddAll, "{..,\"string\":\"stringEntry\",\"number\":2,\"boolean\":false}"},
                {"map_delta_update_all_key", mapDeltaUpdateAll, "{..,\"string\":{\"string\":\"data-team-qa\"},\"number\":{\"number\":1},\"boolean\":{\"boolean\":false}}"},
                {"map_delta_null_key", mapDeltaNull, "{..,\"title\":null}"},
                {"map_delta_remove_key", mapDeltaRemove, "{..,\"data-team-qa\":~}"},
                {"map_delta_remove_all_key", mapDeltaRemoveAll, "{..,\"data-team-qa\":~,\"emodb\":~}"},
                {"map_delta_retain_key", mapDeltaRetain, "{..,\"data-team-qa\":..}"},
                {"map_delta_retain_remove_key", mapDeltaRetainRemove, "{..,\"data-team-qa\":..,\"emodb\":~}"},
                {"map_delta_retain_remove_all_key", mapDeltaRetainRemoveRest, "{\"data-team-qa\":..}"},

                {"set_delta_strings_key", setDeltaStrings, "{..,\"strings\":(..,\"data-team-qa\",\"emodb\")}"},
                {"set_delta_number_key", setDeltaNumber, "{..,\"number\":(..,230)}"},
                {"set_delta_numbers_key", setDeltaNumbers, "{..,\"numbers\":(..,7546, 112)}"},
                {"set_delta_strings_numbers_key", setDeltaStringsNumbers, "{..,\"numbers\":(..,7546, \"data-team-qa\", 112, \"emodb\")}"},
                {"set_delta_all_things_key", setDeltaAddAll, "{..,\"all\":(..,7546, \"data-team-qa\", true, false, null, \"emodb\")}"},
                {"set_delta_remove_key", setDeltaRemove, "{..,\"remove\":(..,~\"data-team-qa\")}"},
                {"set_delta_remove_all_key", setDeltaRemoveAll, "{..,\"removeAll\":(..,~\"data-team-qa\", ~\"emodb\")}"},
        };
    }

    @Test(dataProvider = "dataprovider_testDeltas")
    public void testDeltas(String key, Delta delta, String expectedDeltaString) {
        Delta expectedDelta = Deltas.fromString(expectedDeltaString);
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, key, "delta", runID);
        String tableName = createTable(helper);

        updateDocument(dataStore, tableName, key, delta);
        List<Change> compactedTimelineList = getTimelineChangeList(dataStore, tableName, key, true, false);
        assertEquals(compactedTimelineList.size(), 1);
        assertEquals(compactedTimelineList.get(0).getDelta(), expectedDelta);
    }

    /*
     *  This test is to make sure that D3 improvement (lazy json) works and gives proper documents.
     */
    @Test
    public void testD3Delta() {
        DataStoreHelper helper = new DataStoreHelper(dataStore, placement, "delta_d3", "delta_d3", runID);
        final String tableName = createTable(helper);

        Delta firstDelta = Deltas.literal(ImmutableMap.of("field1", "1", "field2", "2"));
        updateDocument(dataStore, tableName, helper.getDocumentKey(), firstDelta);

        Map<String, Object> returnedDoc = dataStore.get(tableName, helper.getDocumentKey());
        assertTrue(returnedDoc.keySet().containsAll(Lists.newArrayList("field1", "field2")));
        assertEquals(returnedDoc.get("field1"), "1");
        assertEquals(returnedDoc.get("field2"), "2");

        Delta secondDelta = Deltas.mapBuilder().putAll(ImmutableMap.of("field1", "3", "new_field", "true")).build();
        updateDocument(dataStore, tableName, helper.getDocumentKey(), secondDelta);

        returnedDoc = dataStore.get(tableName, helper.getDocumentKey());
        assertTrue(returnedDoc.keySet().containsAll(Lists.newArrayList("field1", "field2", "new_field")));
        assertEquals(returnedDoc.get("field1"), "3");
        assertEquals(returnedDoc.get("field2"), "2");
        assertEquals(returnedDoc.get("new_field"), "true");
    }
}
