package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.bazaarvoice.emodb.sor.db.test.InMemoryDataReaderDAO;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.test.SystemClock;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class SorUpdateTest {
    private static final String TABLE_NAME = "test:table";
    private static final String PLACEMENT = "app_global:default";

    private DataStore _dataStore;
    private UpdateIntentEvent _updateIntentEvent;

    @BeforeTest
    public void SetupTest() {
        final InMemoryDataReaderDAO dataDAO = new InMemoryDataReaderDAO();
        DatabusEventWriterRegistry eventWriterRegistry = new DatabusEventWriterRegistry();
        eventWriterRegistry.registerDatabusEventWriter(event -> _updateIntentEvent = event);
        _dataStore = new InMemoryDataStore(eventWriterRegistry, dataDAO, new MetricRegistry());


        // Create a table for our test
        _dataStore.createTable(TABLE_NAME,
                new TableOptionsBuilder().setPlacement(PLACEMENT).build(),
                ImmutableMap.<String, Object>of(),
                new AuditBuilder().setComment("Updates test").build());
    }

    @Test
    public void testUpdatesAndDatabusEvents() {
        resetValues();
        UUID changeId1 = TimeUUIDs.newUUID();
        SystemClock.tick();
        UUID changeId2 = TimeUUIDs.newUUID();
        _dataStore.update("test:table", "rowkey", changeId1, Deltas.mapBuilder()
                .update("test", Deltas.literal("testValue"))
                .build(),
                new AuditBuilder().setComment("Update").build());

        // Verify that databus would receive the correct UpdateIntentEvent
        UpdateIntentEvent expectedUpdateIntentEvent = new UpdateIntentEvent(_dataStore, Lists.newArrayList(
                new UpdateRef("test:table", "rowkey", changeId1, ImmutableSet.<String>of())));
        assertEquals(_updateIntentEvent.getUpdateRefs(), expectedUpdateIntentEvent.getUpdateRefs(), "Expected events not generated on databus");

        // Try updating the event but suppress databus events
        resetValues();
        _dataStore.updateAll(Lists.newArrayList(new Update("test:table", "rowkey", changeId2,
                        Deltas.mapBuilder()
                                .update("test", Deltas.literal("testValue"))
                                .build(),
                        new AuditBuilder().setComment("Update").build())),
                ImmutableSet.of("ignore"));

        // Verify that the ignorable flag is set
        expectedUpdateIntentEvent = new UpdateIntentEvent(_dataStore, Lists.newArrayList(new UpdateRef("test:table", "rowkey", changeId2, ImmutableSet.of("ignore"))));
        assertEquals(_updateIntentEvent.getUpdateRefs(), expectedUpdateIntentEvent.getUpdateRefs(), "Expected events not generated on databus");
    }

    private void resetValues() {
        _updateIntentEvent = null;
    }
}
