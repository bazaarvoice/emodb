package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.event.api.BaseEventStore;
import com.bazaarvoice.emodb.queue.core.kafka.KafkaProducerService;
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
import java.util.Collection;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.collections.Lists;

import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class SorUpdateTest {
    private static final String TABLE_NAME = "test:table";
    private static final String PLACEMENT = "app_global:default";

    private DataStore _dataStore;
    private DatabusEventWriterRegistry _eventWriterRegistry;
    private Collection<UpdateRef> _updateRefs;

    @BeforeMethod
    public void SetupTest() {
        final InMemoryDataReaderDAO dataDAO = new InMemoryDataReaderDAO();
        _eventWriterRegistry = new DatabusEventWriterRegistry();
        _dataStore = new InMemoryDataStore(_eventWriterRegistry, dataDAO, new MetricRegistry(), new KafkaProducerService(), mock(BaseEventStore.class));


        // Create a table for our test
        _dataStore.createTable(TABLE_NAME,
                new TableOptionsBuilder().setPlacement(PLACEMENT).build(),
                ImmutableMap.<String, Object>of(),
                new AuditBuilder().setComment("Updates test").build());
    }

    @Test
    public void testUpdatesAndDatabusEvents() {
        _eventWriterRegistry.registerDatabusEventWriter(refs -> _updateRefs = refs);
        _updateRefs = null;
        UUID changeId1 = TimeUUIDs.newUUID();
        SystemClock.tick();
        UUID changeId2 = TimeUUIDs.newUUID();
        _dataStore.update("test:table", "rowkey", changeId1, Deltas.mapBuilder()
                .update("test", Deltas.literal("testValue"))
                .build(),
                new AuditBuilder().setComment("Update").build());

        // Verify that databus would receive the correct UpdateRefs
        assertEquals(_updateRefs, Lists.newArrayList(
                new UpdateRef("test:table", "rowkey", changeId1, ImmutableSet.<String>of())),
                "Expected events not generated on databus");

        // Try updating the event but suppress databus events
        _updateRefs = null;
        _dataStore.updateAll(Lists.newArrayList(new Update("test:table", "rowkey", changeId2,
                        Deltas.mapBuilder()
                                .update("test", Deltas.literal("testValue"))
                                .build(),
                        new AuditBuilder().setComment("Update").build())),
                ImmutableSet.of("ignore"));

        // Verify that the ignorable flag is set
        assertEquals(_updateRefs,
                Lists.newArrayList(new UpdateRef("test:table", "rowkey", changeId2, ImmutableSet.of("ignore"))),
                "Expected events not generated on databus");
    }


    @Test
    public void testFailedDatabus() {

        _dataStore.update("test:table", "rowkey", TimeUUIDs.newUUID(), Deltas.mapBuilder()
                        .update("test", Deltas.literal("foo"))
                        .build(),
                new AuditBuilder().setComment("This update should succeed").build());

        _eventWriterRegistry.registerDatabusEventWriter(refs -> {
            throw new RuntimeException();
        });

        try {
            _dataStore.update("test:table", "rowkey", TimeUUIDs.newUUID(), Deltas.mapBuilder()
                            .update("test", Deltas.literal("testValue"))
                            .build(),
                    new AuditBuilder().setComment("This update should fail").build());
            fail();
        } catch (RuntimeException e) {
            assertEquals(_dataStore.get("test:table", "rowkey").get("test"), "foo",
                    "Record should not have been updated after databus event write failed");
        }
    }
}
