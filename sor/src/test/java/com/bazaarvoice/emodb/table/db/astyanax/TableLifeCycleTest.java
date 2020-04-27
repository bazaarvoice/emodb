package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.task.TaskRegistry;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.core.DefaultDataCenter;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.FacadeExistsException;
import com.bazaarvoice.emodb.sor.api.FacadeOptions;
import com.bazaarvoice.emodb.sor.api.FacadeOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.api.UnknownFacadeException;
import com.bazaarvoice.emodb.sor.api.UnpublishedDatabusEvent;
import com.bazaarvoice.emodb.sor.api.UnpublishedDatabusEventType;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.test.InMemoryDataStore;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.MapDeltaBuilder;
import com.bazaarvoice.emodb.table.db.MoveType;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableBackingStore;
import com.bazaarvoice.emodb.table.db.eventregistry.TableEvent;
import com.bazaarvoice.emodb.table.db.eventregistry.TableEventRegistry;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.fest.assertions.api.Assertions;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.DROPPED;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.MIRROR_ACTIVATED;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.MIRROR_CONSISTENT;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.MIRROR_COPIED;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.MIRROR_CREATED;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.MIRROR_DEMOTED;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.MIRROR_EXPIRED;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.MIRROR_EXPIRING;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.PRIMARY;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.PURGED_1;
import static com.bazaarvoice.emodb.table.db.astyanax.StorageState.PURGED_2;
import static com.bazaarvoice.emodb.table.db.eventregistry.TableEvent.Action.DROP;
import static com.bazaarvoice.emodb.table.db.eventregistry.TableEvent.Action.PROMOTE;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Tests the life cycle of SoR and Blob tables: create, move, drop operations.
 */
public class TableLifeCycleTest {
    public static final String DC_US = "us-1";
    public static final String DC_EU = "eu-2";
    public static final String DC_SA = "sa-3";
    public static final String DC_ZZ = "zz-4";

    public static final String PL_GLOBAL = "global";
    public static final String PL_US_EU = "us-eu";
    public static final String PL_EU_APAC = "eu-apac";
    public static final String PL_US = "us";
    public static final String PL_EU = "eu";
    public static final String PL_APAC = "apac";
    public static final String PL_ZZ = "zz";
    public static final String PL_ZZ_MOVING = "zz_moving";

    public static final String TABLE = "my:table";
    public static final String TABLE1 = "my1:table";
    public static final String TABLE2 = "my2:table";
    public static final String TABLE3 = "my3:table";
    public static final String TABLE4 = "my4:table";
    public static final String TABLE5 = "my5:table";
    public static final String TABLE6 = "my6:table";


    @Test
    public void testCreate()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of("space", "test"), newAudit());

        TableJson table = tableDAO.readTableJson(TABLE, true);

        // Verify top-level attributes.
        String uuid = checkNotNull(table.getUuidString());
        assertEquals(table.getAttributeMap(), ImmutableMap.<String, Object>of("space", "test"));
        assertEquals(table.getStorages().size(), 1);
        assertFalse(table.isDeleted());
        assertFalse(table.isDropped());

        // Verify storage-level attributes.
        Storage storage = checkNotNull(table.getMasterStorage());
        assertEquals(storage.getUuidString(), uuid);
        assertEquals(storage.getState(), PRIMARY);
        assertEquals(storage.getRawJson(), ImmutableMap.of(
                "placement", PL_US,
                "shards", 16));
    }

    @Test
    public void testCreateFacade()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of("space", "test"), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_EU), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_APAC), newAudit());

        TableJson table = tableDAO.readTableJson(TABLE, true);

        // Verify top-level attributes.
        String uuid = checkNotNull(table.getUuidString());
        assertEquals(table.getAttributeMap(), ImmutableMap.<String, Object>of("space", "test"));
        assertEquals(table.getStorages().size(), 3);

        // Verify storage-level attributes.
        Storage storage = checkNotNull(table.getMasterStorage());
        assertEquals(storage.getUuidString(), uuid);
        assertEquals(storage.getState(), PRIMARY);
        assertEquals(storage.getRawJson(), ImmutableMap.of(
                "placement", PL_US,
                "shards", 16));

        assertEquals(table.getFacades().size(), 2);
        Storage facadeEu = table.getFacadeForPlacement(PL_EU);
        Storage facadeApac = table.getFacadeForPlacement(PL_APAC);
        assertEquals(facadeEu.getState(), PRIMARY);
        assertEquals(facadeEu.getRawJson(), ImmutableMap.of(
                "facade", true,
                "placement", PL_EU,
                "shards", 16));
        assertEquals(facadeApac.getState(), PRIMARY);
        assertEquals(facadeApac.getRawJson(), ImmutableMap.of(
                "facade", true,
                "placement", PL_APAC,
                "shards", 16));
    }

    @Test
    public void testDrop()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of("space", "test"), newAudit());
        String uuid = checkNotNull(tableDAO.readTableJson(TABLE, true).getUuidString());

        // Drop the table
        Instant start = Instant.now();
        tableDAO.drop(TABLE, newAudit());
        Instant end = Instant.now();

        TableJson table = tableDAO.readTableJson(TABLE, false);
        assertNull(tableDAO.tableFromJson(table));

        // Verify top-level attributes.
        assertNull(table.getUuidString());
        assertEquals(table.getAttributeMap(), null);
        assertEquals(table.getStorages().size(), 1);

        // Verify storage-level attributes.
        assertNull(table.getMasterStorage());
        assertEquals(table.getFacades().size(), 0);
        Storage storage = checkNotNull(getStorage(table, uuid));
        Instant droppedAt = storage.getTransitionedTimestamp(DROPPED);
        assertEquals(storage.getUuidString(), uuid);
        assertEquals(storage.getState(), DROPPED);
        assertEquals(storage.getRawJson(), ImmutableMap.of(
                "placement", PL_US,
                "shards", 16,
                "droppedAt", formatTimestamp(droppedAt)));
        assertTrue(storage.isDropped());
        assertBetween(start, droppedAt, end);
    }

    @Test
    public void testDropPurgeDelete()
            throws Exception {
        InMemoryDataStore backingStore = newBackingStore(new MetricRegistry());
        Date fct = new Date(0);
        AstyanaxTableDAO tableDAO = newTableDAO(backingStore, DC_US, mock(DataCopyDAO.class), mock(DataPurgeDAO.class), fct);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of("space", "test"), newAudit());
        registerTableEventListeners(backingStore);
        String uuid = checkNotNull(tableDAO.readTableJson(TABLE, true).getUuidString());
        tableDAO.drop(TABLE, newAudit());
        Instant droppedAt = getStorage(tableDAO.readTableJson(TABLE, false), uuid)
                .getTransitionedTimestamp(DROPPED);

        // Before the elapsed time has passed, doing maintenance shouldn't change anything.
        assertNoopMetadataMaintenance(backingStore, DC_US, TABLE);
        assertNoopDataMaintenance(backingStore, DC_US, TABLE);

        // Do maintenance from the US.  It should do the initial purge of the data.
        Instant purgedAt1;
        {
            // Hack the table JSON to pretend the drop occurred long enough ago that now it's time to do the purge.
            droppedAt = droppedAt.minus(AstyanaxTableDAO.DROP_TO_PURGE_1);
            patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.droppedAt", uuid), droppedAt);

            // Maintenance from the EU should no-op.  Metadata maintenance should no-op.
            assertNoopMetadataMaintenance(backingStore, DC_EU, TABLE);
            assertNoopDataMaintenance(backingStore, DC_EU, TABLE);
            assertNoopMetadataMaintenance(backingStore, DC_US, TABLE);

            DataCopyDAO usDataCopyDAO = mock(DataCopyDAO.class);
            DataPurgeDAO usDataPurgeDAO = mock(DataPurgeDAO.class);
            AstyanaxTableDAO usTableDAO = newTableDAO(backingStore, DC_US, usDataCopyDAO, usDataPurgeDAO, fct);

            // The purge is delayed until full consistency is achieved relative to the time of the drop.
            try {
                usTableDAO.performDataMaintenance(TABLE, mock(Runnable.class));
                fail();
            } catch (FullConsistencyException e) {
                // Expected
            }
            fct.setTime(droppedAt.toEpochMilli() + 1);

            assertReadyTableEventAbsent(backingStore, DC_US);
            assertReadyTableEventAbsent(backingStore, DC_EU);
            assertReadyTableEventAbsent(backingStore, DC_SA);
            assertReadyTableEventAbsent(backingStore, DC_ZZ);

            try {
                usTableDAO.performDataMaintenance(TABLE, mock(Runnable.class));
                fail();
            } catch (PendingTableEventsException e) {
                // Expected
            }

            assertReadyTableEventPresent(backingStore, DC_US, TABLE, uuid, DROP);
            assertReadyTableEventAbsent(backingStore, DC_US);

            // Next do data maintenance from the US.  It should do the purge and nothing else.
            Instant start = Instant.now();
            usTableDAO.performDataMaintenance(TABLE, mock(Runnable.class));
            Instant end = Instant.now();

            // Did we mark the purge as having occurred?
            TableJson table = tableDAO.readTableJson(TABLE, false);

            Storage storage = getStorage(table, uuid);
            assertEquals(storage.getState(), PURGED_1);
            purgedAt1 = storage.getTransitionedTimestamp(PURGED_1);
            assertBetween(start, purgedAt1, end);
            assertNull(storage.getTransitionedTimestamp(PURGED_2));

            // Did the purge method get called?
            ArgumentCaptor<AstyanaxStorage> storageCapture = ArgumentCaptor.forClass(AstyanaxStorage.class);
            verify(usDataPurgeDAO).purge(storageCapture.capture(), Mockito.<Runnable>any());
            assertTrue(storageCapture.getValue().hasUUID(getStorage(table, uuid).getUuid()));
            verifyNoMoreInteractions(usDataCopyDAO, usDataPurgeDAO);
        }

        // Before the elapsed time has passed, doing maintenance shouldn't change anything.
        assertNoopMetadataMaintenance(backingStore, DC_US, TABLE);
        assertNoopDataMaintenance(backingStore, DC_US, TABLE);

        // Do maintenance from the US.  It should do the final purge of the data.
        Instant purgedAt2;
        {
            // Hack the table JSON to pretend the drop occurred long enough ago that now it's time to do the purge.
            droppedAt = droppedAt.minus(AstyanaxTableDAO.DROP_TO_PURGE_2);
            patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.droppedAt", uuid), droppedAt);

            DataCopyDAO usDataCopyDAO = mock(DataCopyDAO.class);
            DataPurgeDAO usDataPurgeDAO = mock(DataPurgeDAO.class);
            AstyanaxTableDAO usTableDAO = newTableDAO(backingStore, DC_US, usDataCopyDAO, usDataPurgeDAO, fct);

            // Next do data maintenance from the US.  It should do the purge and nothing else.
            Instant start = Instant.now();
            usTableDAO.performDataMaintenance(TABLE, mock(Runnable.class));
            Instant end = Instant.now();

            TableJson table = tableDAO.readTableJson(TABLE, false);
            Storage storage = checkNotNull(getStorage(table, uuid));
            purgedAt2 = storage.getTransitionedTimestamp(PURGED_2);
            assertBetween(start, purgedAt2, end);
            assertEquals(storage.getTransitionedTimestamp(PURGED_1), purgedAt1);

            // Verify top-level attributes.
            assertNull(table.getUuidString());
            assertEquals(table.getAttributeMap(), null);
            assertEquals(table.getStorages().size(), 1);

            // Verify storage-level attributes.
            assertNull(table.getMasterStorage());
            droppedAt = storage.getTransitionedTimestamp(DROPPED);
            assertEquals(storage.getUuidString(), uuid);
            assertEquals(storage.getState(), PURGED_2);
            assertEquals(storage.getRawJson(), ImmutableMap.of(
                    "placement", PL_US,
                    "shards", 16,
                    "droppedAt", formatTimestamp(droppedAt),
                    "purgedAt1", formatTimestamp(purgedAt1),
                    "purgedAt2", formatTimestamp(purgedAt2)));
            assertTrue(storage.isDropped());
        }

        // Before the elapsed time has passed, doing maintenance shouldn't change anything.
        assertNoopMetadataMaintenance(backingStore, DC_US, TABLE);
        assertNoopDataMaintenance(backingStore, DC_US, TABLE);

        // Do maintenance from the US.  It should delete the json metadata completely.
        {
            // Hack the table JSON to pretend the drop occurred long enough ago that now it's time to do the delete.
            purgedAt2 = purgedAt2.minus(AstyanaxTableDAO.MIN_CONSISTENCY_DELAY);
            patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.purgedAt2", uuid), purgedAt2);

            // Data maintenance should no-op.
            assertNoopDataMaintenance(backingStore, DC_US, TABLE);

            DataCopyDAO usDataCopyDAO = mock(DataCopyDAO.class);
            DataPurgeDAO usDataPurgeDAO = mock(DataPurgeDAO.class);
            AstyanaxTableDAO usTableDAO = newTableDAO(backingStore, DC_US, usDataCopyDAO, usDataPurgeDAO, fct);

            // Next do metadata maintenance from the US.  It should delete the table metadata.
            Instant start = Instant.now();
            usTableDAO.performMetadataMaintenance(TABLE);
            Instant end = Instant.now();

            // Is all the data gone?
            TableJson table = tableDAO.readTableJson(TABLE, false);
            assertTrue(table.isDeleted());
            assertTrue(table.isDropped());
            assertNull(table.getRawJson().get("uuid"));
            assertNull(table.getRawJson().get("attributes"));
            assertNull(table.getRawJson().get("storage"));
            assertBetween(start, Intrinsic.getLastUpdateAt(table.getRawJson()).toInstant(), end);
        }
    }

    @Test
    public void testDropFacade()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_APAC), ImmutableMap.<String, Object>of("space", "test"), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_US), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_EU), newAudit());
        TableJson table = tableDAO.readTableJson(TABLE, true);
        String masterUuid = checkNotNull(table.getUuidString());
        String usUuid = checkNotNull(table.getFacadeForPlacement(PL_US).getUuidString());
        String euUuid = checkNotNull(table.getFacadeForPlacement(PL_EU).getUuidString());

        // Drop a facade
        Instant start = Instant.now();
        tableDAO.dropFacade(TABLE, PL_US, newAudit());
        Instant end = Instant.now();

        table = tableDAO.readTableJson(TABLE, false);
        Instant droppedAt = checkNotNull(getStorage(table, usUuid).getTransitionedTimestamp(DROPPED));

        // Verify top-level attributes.
        assertEquals(table.getUuidString(), masterUuid);
        assertEquals(table.getAttributeMap(), ImmutableMap.<String, Object>of("space", "test"));
        assertEquals(table.getStorages().size(), 3);

        // Verify storage-level attributes.
        Storage master = checkNotNull(table.getMasterStorage());
        assertEquals(master.getUuidString(), masterUuid);
        assertEquals(master.getState(), PRIMARY);
        assertEquals(master.getRawJson(), ImmutableMap.of(
                "placement", PL_APAC,
                "shards", 16));
        Storage us = checkNotNull(getStorage(table, usUuid));
        assertEquals(us.getUuidString(), usUuid);
        assertEquals(us.getState(), DROPPED);
        assertEquals(us.getRawJson(), ImmutableMap.of(
                "facade", true,
                "placement", PL_US,
                "shards", 16,
                "droppedAt", formatTimestamp(droppedAt)));
        Storage eu = checkNotNull(getStorage(table, euUuid));
        assertEquals(eu.getUuidString(), euUuid);
        assertEquals(eu.getState(), PRIMARY);
        assertEquals(eu.getRawJson(), ImmutableMap.of(
                "facade", true,
                "placement", PL_EU,
                "shards", 16));
        assertTrue(us.isDropped());
        assertBetween(start, droppedAt, end);

        try {
            table.getFacadeForPlacement(PL_US);
            fail();
        } catch (UnknownFacadeException e) {
            assertEquals(e.getMessage(), "Unknown facade: my:table in us");
        }
        assertEquals(table.getFacadeForPlacement(PL_EU).getUuidString(), euUuid);
    }

    @Test
    public void testDropFacadePurgeDelete()
            throws Exception {
        InMemoryDataStore backingStore = newBackingStore(new MetricRegistry());
        Date fct = new Date(0);
        AstyanaxTableDAO tableDAO = newTableDAO(backingStore, DC_US, mock(DataCopyDAO.class), mock(DataPurgeDAO.class), fct);
        registerTableEventListeners(backingStore);
        tableDAO.create(TABLE, newOptions(PL_APAC), ImmutableMap.<String, Object>of("space", "test"), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_US), newAudit());
        TableJson table = tableDAO.readTableJson(TABLE, true);
        String masterUuid = checkNotNull(table.getUuidString());
        String facadeUuid = checkNotNull(table.getFacadeForPlacement(PL_US).getUuidString());
        tableDAO.dropFacade(TABLE, PL_US, newAudit());
        Instant droppedAt = getStorage(tableDAO.readTableJson(TABLE, false), facadeUuid).getTransitionedTimestamp(DROPPED);

        // Before the elapsed time has passed, doing maintenance shouldn't change anything.
        assertNoopMetadataMaintenance(backingStore, DC_US, TABLE);
        assertNoopDataMaintenance(backingStore, DC_US, TABLE);

        // Do maintenance from the US.  It should do the initial purge of the data.
        Instant purgedAt1;
        {
            // Hack the table JSON to pretend the drop occurred long enough ago that now it's time to do the purge.
            droppedAt = droppedAt.minus(AstyanaxTableDAO.DROP_TO_PURGE_1);
            patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.droppedAt", facadeUuid), droppedAt);

            // Maintenance from the EU should no-op.  Metadata maintenance should no-op.
            assertNoopMetadataMaintenance(backingStore, DC_EU, TABLE);
            assertNoopDataMaintenance(backingStore, DC_EU, TABLE);
            assertNoopMetadataMaintenance(backingStore, DC_US, TABLE);

            DataCopyDAO usDataCopyDAO = mock(DataCopyDAO.class);
            DataPurgeDAO usDataPurgeDAO = mock(DataPurgeDAO.class);
            AstyanaxTableDAO usTableDAO = newTableDAO(backingStore, DC_US, usDataCopyDAO, usDataPurgeDAO, fct);
            fct.setTime(droppedAt.toEpochMilli() + 1);

            assertReadyTableEventAbsent(backingStore, DC_US);
            assertReadyTableEventAbsent(backingStore, DC_EU);
            assertReadyTableEventAbsent(backingStore, DC_SA);
            assertReadyTableEventAbsent(backingStore, DC_ZZ);


            try {
                usTableDAO.performDataMaintenance(TABLE, mock(Runnable.class));
                fail();
            } catch (PendingTableEventsException e) {
                // Expected
            }

            assertReadyTableEventPresent(backingStore, DC_US, TABLE, facadeUuid, DROP);
            assertReadyTableEventAbsent(backingStore, DC_EU);
            assertReadyTableEventAbsent(backingStore, DC_SA);
            assertReadyTableEventAbsent(backingStore, DC_ZZ);

            // Next do data maintenance from the US.  It should do the purge and nothing else.
            Instant start = Instant.now();
            usTableDAO.performDataMaintenance(TABLE, mock(Runnable.class));
            Instant end = Instant.now();

            // Did we mark the purge as having occurred?
            table = tableDAO.readTableJson(TABLE, false);
            Storage facade = getStorage(table, facadeUuid);
            assertEquals(facade.getState(), PURGED_1);
            purgedAt1 = facade.getTransitionedTimestamp(PURGED_1);
            assertBetween(start, purgedAt1, end);
            assertNull(facade.getTransitionedTimestamp(PURGED_2));

            // Did the purge method get called?
            ArgumentCaptor<AstyanaxStorage> storageCapture = ArgumentCaptor.forClass(AstyanaxStorage.class);
            verify(usDataPurgeDAO).purge(storageCapture.capture(), Mockito.<Runnable>any());
            assertTrue(storageCapture.getValue().hasUUID(facade.getUuid()));
            verifyNoMoreInteractions(usDataCopyDAO, usDataPurgeDAO);
        }

        // Before the elapsed time has passed, doing maintenance shouldn't change anything.
        assertNoopMetadataMaintenance(backingStore, DC_US, TABLE);
        assertNoopDataMaintenance(backingStore, DC_US, TABLE);

        // Do maintenance from the US.  It should do the final purge of the data.
        Instant purgedAt2;
        {
            // Hack the table JSON to pretend the drop occurred long enough ago that now it's time to do the purge.
            droppedAt = droppedAt.minus(AstyanaxTableDAO.DROP_TO_PURGE_2);
            patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.droppedAt", facadeUuid), droppedAt);

            DataCopyDAO usDataCopyDAO = mock(DataCopyDAO.class);
            DataPurgeDAO usDataPurgeDAO = mock(DataPurgeDAO.class);
            AstyanaxTableDAO usTableDAO = newTableDAO(backingStore, DC_US, usDataCopyDAO, usDataPurgeDAO, fct);

            // Next do data maintenance from the US.  It should do the purge and nothing else.
            Instant start = Instant.now();
            usTableDAO.performDataMaintenance(TABLE, mock(Runnable.class));
            Instant end = Instant.now();

            table = tableDAO.readTableJson(TABLE, false);
            Storage storage = getStorage(table, facadeUuid);
            purgedAt2 = storage.getTransitionedTimestamp(PURGED_2);
            assertBetween(start, purgedAt2, end);
            assertEquals(storage.getTransitionedTimestamp(PURGED_1), purgedAt1);

            // Verify top-level attributes.
            assertFalse(table.isDropped());
            assertEquals(table.getUuidString(), masterUuid);
            assertEquals(table.getAttributeMap(), ImmutableMap.<String, Object>of("space", "test"));
            assertEquals(table.getStorages().size(), 2);

            // Verify storage-level attributes.
            assertEquals(table.getMasterStorage().getRawJson(), ImmutableMap.of(
                    "placement", PL_APAC,
                    "shards", 16));
            Storage facade = checkNotNull(getStorage(table, facadeUuid));
            droppedAt = facade.getTransitionedTimestamp(DROPPED);
            assertEquals(facade.getUuidString(), facadeUuid);
            assertEquals(facade.getState(), PURGED_2);
            assertEquals(facade.getRawJson(), ImmutableMap.builder()
                    .put("facade", true)
                    .put("placement", PL_US)
                    .put("shards", 16)
                    .put("droppedAt", formatTimestamp(droppedAt))
                    .put("purgedAt1", formatTimestamp(purgedAt1))
                    .put("purgedAt2", formatTimestamp(purgedAt2))
                    .build());
            assertTrue(facade.isDropped());
        }

        // Before the elapsed time has passed, doing maintenance shouldn't change anything.
        assertNoopMetadataMaintenance(backingStore, DC_US, TABLE);
        assertNoopDataMaintenance(backingStore, DC_US, TABLE);

        // Do maintenance from the US.  It should completely delete the json metadata for the facade.
        {
            // Hack the table JSON to pretend the drop occurred long enough ago that now it's time to do the delete.
            purgedAt2 = purgedAt2.minus(AstyanaxTableDAO.MIN_CONSISTENCY_DELAY);
            patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.purgedAt2", facadeUuid), purgedAt2);

            // Data maintenance should no-op.
            assertNoopDataMaintenance(backingStore, DC_US, TABLE);

            DataCopyDAO usDataCopyDAO = mock(DataCopyDAO.class);
            DataPurgeDAO usDataPurgeDAO = mock(DataPurgeDAO.class);
            AstyanaxTableDAO usTableDAO = newTableDAO(backingStore, DC_US, usDataCopyDAO, usDataPurgeDAO, fct);

            // Next do metadata maintenance from the US.  It should delete the facade metadata.
            Instant start = Instant.now();
            usTableDAO.performMetadataMaintenance(TABLE);
            Instant end = Instant.now();

            // Verify top-level attributes.
            table = tableDAO.readTableJson(TABLE, false);
            assertEquals(table.getUuidString(), masterUuid);
            assertEquals(table.getAttributeMap(), ImmutableMap.<String, Object>of("space", "test"));
            assertEquals(table.getStorages().size(), 1);
            assertFalse(table.isDeleted());
            assertFalse(table.isDropped());

            // Verify storage-level attributes.
            assertEquals(table.getMasterStorage().getRawJson(), ImmutableMap.of(
                    "placement", PL_APAC,
                    "shards", 16));
            assertNull(getStorage(table, facadeUuid));
            assertBetween(start, Intrinsic.getLastUpdateAt(table.getRawJson()).toInstant(), end);
        }
    }

    @Test
    public void testMoveStart()
            throws Exception {
        InMemoryDataStore backingStore = newBackingStore(new MetricRegistry());
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US, backingStore);
        registerTableEventListeners(backingStore);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());
        String srcUuid = checkNotNull(tableDAO.readTableJson(TABLE, true).getUuidString());

        Instant start = Instant.now();
        tableDAO.move(TABLE, PL_GLOBAL, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);
        Instant end = Instant.now();

        TableJson table = tableDAO.readTableJson(TABLE, true);
        String destUuid = table.getMasterStorage().getMoveTo().getUuidString();
        AstyanaxTable astyanaxTable = (AstyanaxTable) tableDAO.tableFromJson(table);

        // Verify top-level attributes.
        assertEquals(table.getUuidString(), srcUuid);
        assertEquals(table.getAttributeMap(), ImmutableMap.<String, Object>of());
        assertEquals(table.getStorages().size(), 2);

        // Verify storage-level attributes.
        Storage src = checkNotNull(table.getMasterStorage());
        Storage dest = checkNotNull(src.getMoveTo());
        assertEquals(src.getUuidString(), srcUuid);
        assertEquals(src.getState(), PRIMARY);
        assertTrue(src.isPrimary());
        assertEquals(src.getRawJson(), ImmutableMap.<String, Object>builder()
                .put("placement", PL_US)
                .put("shards", 16)
                .put("moveTo", dest.getUuidString())
                .build());
        assertEquals(src.getMirrors().size(), 1);
        assertEquals(dest.getRawJson(), ImmutableMap.<String, Object>builder()
                .put("placement", PL_GLOBAL)
                .put("shards", 16)
                .put("groupId", srcUuid)
                .put("mirrorCreatedAt", dest.getRawJson().get("mirrorCreatedAt"))
                .put("mirrorActivatedAt", dest.getRawJson().get("mirrorActivatedAt"))
                .build());
        assertEquals(dest.getState(), MIRROR_ACTIVATED);
        assertFalse(dest.isPrimary());
        assertEquals(dest.getPrimary(), src);
        assertBetween(start, dest.getTransitionedTimestamp(MIRROR_CREATED), end);
        assertBetween(start, dest.getTransitionedTimestamp(MIRROR_ACTIVATED), end);

        // Verify that read/write mirroring is going to the right place.
        assertTrue(astyanaxTable.getReadStorage().hasUUID(src.getUuid()));
        assertStorageUuids(astyanaxTable.getWriteStorage(), src.getUuid(), dest.getUuid());

        // Verify that the next step in the move was scheduled as expected.
        MaintenanceOp maintenanceOp = tableDAO.getNextMaintenanceOp(TABLE);
        assertMaintenance(maintenanceOp, "Move:copy-data", MaintenanceType.DATA, DC_US);
        assertBetween(start, maintenanceOp.getWhen(), end, AstyanaxTableDAO.MIN_CONSISTENCY_DELAY);

        assertTableEventsPresent(backingStore, ImmutableSet.of(DC_EU, DC_SA, DC_ZZ), TABLE, destUuid, PROMOTE);
        assertTableEventsAbsent(backingStore, ImmutableSet.of(DC_US, DC_EU, DC_SA, DC_ZZ), TABLE, srcUuid);
    }

    /**
     * Can't move directly from us to eu since there are no data centers in common.
     */
    @Test
    public void testMoveIncompatiblePlacement()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());

        try {
            tableDAO.move(TABLE, PL_EU, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Source and destination and mirror placements must overlap in some data center: eu, us");
        }
    }

    @Test
    public void testMoveFacadeStart()
            throws Exception {
        InMemoryDataStore backingStore = newBackingStore(new MetricRegistry());
        registerTableEventListeners(backingStore);
        AstyanaxTableDAO tableDAO = newTableDAO(DC_EU, backingStore);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_EU), newAudit());
        TableJson table = tableDAO.readTableJson(TABLE, true);
        String masterUuid = checkNotNull(table.getUuidString());
        String srcUuid = checkNotNull(table.getFacadeForPlacement(PL_EU).getUuidString());

        Instant start = Instant.now();
        tableDAO.moveFacade(TABLE, PL_EU, PL_EU, Optional.of(32), newAudit(), MoveType.SINGLE_TABLE);
        Instant end = Instant.now();

        table = tableDAO.readTableJson(TABLE, true);
        AstyanaxTable astyanaxTable = (AstyanaxTable) tableDAO.tableFromJson(table);

        // Verify top-level attributes.
        assertEquals(table.getUuidString(), masterUuid);
        assertEquals(table.getAttributeMap(), ImmutableMap.<String, Object>of());
        assertEquals(table.getStorages().size(), 3);
        assertEquals(table.getFacades().size(), 1);

        // Verify storage-level attributes.
        Storage master = checkNotNull(table.getMasterStorage());
        Storage src = checkNotNull(getStorage(table, srcUuid));
        Storage dest = checkNotNull(src.getMoveTo());
        assertEquals(master.getUuidString(), masterUuid);
        assertEquals(master.getState(), PRIMARY);
        assertEquals(master.getRawJson(), ImmutableMap.<String, Object>builder()
                .put("placement", PL_US)
                .put("shards", 16)
                .build());
        assertEquals(src.getUuidString(), srcUuid);
        assertEquals(src.getState(), PRIMARY);
        assertEquals(src.getRawJson(), ImmutableMap.<String, Object>builder()
                .put("facade", true)
                .put("placement", PL_EU)
                .put("shards", 16)
                .put("moveTo", dest.getUuidString())
                .build());
        assertEquals(src.getMirrors().size(), 1);
        assertEquals(dest.getRawJson(), ImmutableMap.<String, Object>builder()
                .put("facade", true)
                .put("placement", PL_EU)
                .put("shards", 32)
                .put("groupId", srcUuid)
                .put("mirrorCreatedAt", dest.getRawJson().get("mirrorCreatedAt"))
                .put("mirrorActivatedAt", dest.getRawJson().get("mirrorActivatedAt"))
                .build());
        assertEquals(dest.getState(), MIRROR_ACTIVATED);
        assertFalse(dest.isPrimary());
        assertEquals(dest.getPrimary(), src);
        assertBetween(start, dest.getTransitionedTimestamp(MIRROR_CREATED), end);
        assertBetween(start, dest.getTransitionedTimestamp(MIRROR_ACTIVATED), end);

        // Verify that read/write mirroring is going to the right place.
        assertTrue(astyanaxTable.getReadStorage().hasUUID(src.getUuid()));
        assertStorageUuids(astyanaxTable.getWriteStorage(), src.getUuid(), dest.getUuid());

        // Verify that the next step in the move was scheduled as expected.
        MaintenanceOp maintenanceOp = tableDAO.getNextMaintenanceOp(TABLE);
        assertMaintenance(maintenanceOp, "Move:copy-data", MaintenanceType.DATA, DC_EU);
        assertBetween(start, maintenanceOp.getWhen(), end, AstyanaxTableDAO.MIN_CONSISTENCY_DELAY);

        assertTableEventsAbsent(backingStore, ImmutableSet.of(DC_US, DC_EU, DC_SA, DC_ZZ), TABLE, srcUuid);
        assertTableEventsAbsent(backingStore, ImmutableSet.of(DC_US, DC_EU, DC_SA, DC_ZZ), TABLE, dest.getUuidString());

    }

    @Test
    public void testMoveCopyPromoteDrop()
            throws Exception {
        Date fct = new Date(0);
        InMemoryDataStore backingStore = newBackingStore(new MetricRegistry());
        registerTableEventListeners(backingStore);
        AstyanaxTableDAO tableDAO = newTableDAO(backingStore, DC_US, mock(DataCopyDAO.class), mock(DataPurgeDAO.class), fct);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.move(TABLE, PL_GLOBAL, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);
        TableJson table = tableDAO.readTableJson(TABLE, true);
        String srcUuid = table.getMasterStorage().getUuidString();
        String destUuid = table.getMasterStorage().getMoveTo().getUuidString();
        Instant mirrorCreatedAt = checkNotNull(table.getMasterStorage().getMoveTo().getTransitionedTimestamp(MIRROR_CREATED));
        Instant mirrorActivatedAt = checkNotNull(table.getMasterStorage().getMoveTo().getTransitionedTimestamp(MIRROR_ACTIVATED));

        assertTableEventsPresent(backingStore, ImmutableSet.of(DC_EU, DC_SA, DC_ZZ), TABLE, destUuid, PROMOTE);
        assertTableEventsAbsent(backingStore, ImmutableSet.of(DC_US, DC_EU, DC_SA, DC_ZZ), TABLE, srcUuid);
        assertTableEventsAbsent(backingStore, ImmutableSet.of(DC_US), TABLE, destUuid);

        // Before the elapsed time has passed, doing maintenance shouldn't change anything.
        assertNoopMetadataMaintenance(backingStore, DC_US, TABLE);
        assertNoopDataMaintenance(backingStore, DC_US, TABLE);

        // Do maintenance from the US.  It should copy the data.
        Instant mirrorCopiedAt;
        {
            // Hack the table JSON to pretend the move started long enough ago that now it's time to do the copy.
            mirrorActivatedAt = mirrorActivatedAt.minus(AstyanaxTableDAO.MIN_CONSISTENCY_DELAY);
            patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.mirrorActivatedAt", destUuid), mirrorActivatedAt);

            // Maintenance from the EU should no-op.  Metadata maintenance should no-op.
            assertNoopMetadataMaintenance(backingStore, DC_EU, TABLE);
            assertNoopDataMaintenance(backingStore, DC_EU, TABLE);
            assertNoopMetadataMaintenance(backingStore, DC_US, TABLE);

            DataCopyDAO usDataCopyDAO = mock(DataCopyDAO.class);
            DataPurgeDAO usDataPurgeDAO = mock(DataPurgeDAO.class);
            AstyanaxTableDAO usTableDAO = newTableDAO(backingStore, DC_US, usDataCopyDAO, usDataPurgeDAO, fct);

            // The copy is delayed until full consistency is achieved relative to the time of mirror activation.
            try {
                usTableDAO.performDataMaintenance(TABLE, mock(Runnable.class));
                fail();
            } catch (FullConsistencyException e) {
                // Expected
            }
            fct.setTime(mirrorActivatedAt.toEpochMilli() + 1);

            // Next do data maintenance from the US.  It should do the copy and nothing else.
            Instant start = Instant.now();
            usTableDAO.performDataMaintenance(TABLE, mock(Runnable.class));
            Instant end = Instant.now();

            // Did we mark the copy as having occurred?
            table = tableDAO.readTableJson(TABLE, true);
            assertEquals(table.getMasterStorage().getState(), PRIMARY);
            assertEquals(table.getMasterStorage().getMoveTo().getState(), MIRROR_COPIED);
            mirrorCopiedAt = checkNotNull(table.getMasterStorage().getMoveTo().getTransitionedTimestamp(MIRROR_COPIED));
            assertBetween(start, mirrorCopiedAt, end);

            // Did we copy to the right place?
            ArgumentCaptor<AstyanaxStorage> src = ArgumentCaptor.forClass(AstyanaxStorage.class);
            ArgumentCaptor<AstyanaxStorage> dest = ArgumentCaptor.forClass(AstyanaxStorage.class);
            verify(usDataCopyDAO).copy(src.capture(), dest.capture(), Mockito.<Runnable>any());
            assertTrue(src.getValue().hasUUID(table.getMasterStorage().getUuid()));
            assertTrue(dest.getValue().hasUUID(table.getMasterStorage().getMoveTo().getUuid()));
            assertEquals(src.getValue().getPlacement().getName(), PL_US);
            assertEquals(dest.getValue().getPlacement().getName(), PL_GLOBAL);
            verifyNoMoreInteractions(usDataCopyDAO, usDataPurgeDAO);

            // Is followup maintenance scheduled?
            MaintenanceOp maintenanceOp = tableDAO.getNextMaintenanceOp(TABLE);
            assertMaintenance(maintenanceOp, "Move:data-consistent", MaintenanceType.DATA, DC_US);
            assertBetween(start, maintenanceOp.getWhen(), end, AstyanaxTableDAO.MIN_CONSISTENCY_DELAY);
        }

        // Do maintenance from the US.  It should mark the mirror consistent.
        Instant mirrorConsistentAt;
        {
            // Hack the table JSON to pretend the copy started long enough ago that now it's time to check consistency.
            mirrorCopiedAt = mirrorCopiedAt.minus(AstyanaxTableDAO.MIN_CONSISTENCY_DELAY);
            patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.mirrorCopiedAt", destUuid), mirrorCopiedAt);

            // Maintenance from the EU should no-op.  Metadata maintenance should no-op.
            assertNoopMetadataMaintenance(backingStore, DC_EU, TABLE);
            assertNoopDataMaintenance(backingStore, DC_EU, TABLE);
            assertNoopMetadataMaintenance(backingStore, DC_US, TABLE);

            DataCopyDAO usDataCopyDAO = mock(DataCopyDAO.class);
            DataPurgeDAO usDataPurgeDAO = mock(DataPurgeDAO.class);
            AstyanaxTableDAO usTableDAO = newTableDAO(backingStore, DC_US, usDataCopyDAO, usDataPurgeDAO, fct);

            // The copy is delayed until full consistency is achieved relative to the time of mirror copy.
            try {
                usTableDAO.performDataMaintenance(TABLE, mock(Runnable.class));
                fail();
            } catch (FullConsistencyException e) {
                // Expected
            }
            fct.setTime(mirrorCopiedAt.toEpochMilli() + 1);

            // Next do data maintenance from the US.  It should mark the mirror consistent and nothing else.
            Instant start = Instant.now();
            usTableDAO.performDataMaintenance(TABLE, mock(Runnable.class));
            Instant end = Instant.now();

            // Did we mark things as being consistent?
            table = tableDAO.readTableJson(TABLE, true);
            assertEquals(table.getMasterStorage().getState(), PRIMARY);
            assertEquals(table.getMasterStorage().getMoveTo().getState(), MIRROR_CONSISTENT);
            mirrorConsistentAt = checkNotNull(table.getMasterStorage().getMoveTo().getTransitionedTimestamp(MIRROR_CONSISTENT));
            assertBetween(start, mirrorConsistentAt, end);
            verifyNoMoreInteractions(usDataCopyDAO, usDataPurgeDAO);

            // Is followup maintenance scheduled?
            MaintenanceOp maintenanceOp = tableDAO.getNextMaintenanceOp(TABLE);
            assertMaintenance(maintenanceOp, "Move:promote-mirror", MaintenanceType.METADATA, "<system>");
            assertBetween(start, maintenanceOp.getWhen(), end, AstyanaxTableDAO.MIN_DELAY);
        }

        // Before the elapsed time has passed, doing maintenance shouldn't change anything.
        assertNoopMetadataMaintenance(backingStore, DC_US, TABLE);
        assertNoopDataMaintenance(backingStore, DC_US, TABLE);

        // Do maintenance from the US.  It should promote the mirror and flip the primary & mirror relationship.
        UUID promotionId;
        Instant primaryAt;
        {
            // Hack the table JSON to pretend the move copy finished long enough ago that now it's time to do the flip.
            mirrorConsistentAt = mirrorConsistentAt.minus(AstyanaxTableDAO.MIN_DELAY);
            patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.mirrorConsistentAt", destUuid), mirrorConsistentAt);

            Instant start = Instant.now();
            tableDAO.performMetadataMaintenance(TABLE);
            Instant end = Instant.now();

            table = tableDAO.readTableJson(TABLE, true);

            // Verify top-level attributes.
            assertEquals(table.getUuidString(), destUuid);
            assertEquals(table.getAttributeMap(), ImmutableMap.<String, Object>of());
            assertEquals(table.getStorages().size(), 2);

            // Verify storage-level attributes.
            Storage dest = checkNotNull(table.getMasterStorage());
            Storage src = Iterables.getOnlyElement(dest.getMirrors());
            promotionId = dest.getPromotionId();
            primaryAt = checkNotNull(dest.getTransitionedTimestamp(PRIMARY));
            assertEquals(dest.getUuidString(), destUuid);
            assertEquals(dest.getState(), PRIMARY);
            assertEquals(dest.getRawJson(), ImmutableMap.<String, Object>builder()
                    .put("placement", PL_GLOBAL)
                    .put("shards", 16)
                    .put("groupId", srcUuid)
                    .put("mirrorCreatedAt", formatTimestamp(mirrorCreatedAt))
                    .put("mirrorActivatedAt", formatTimestamp(mirrorActivatedAt))
                    .put("mirrorCopiedAt", formatTimestamp(mirrorCopiedAt))
                    .put("mirrorConsistentAt", formatTimestamp(mirrorConsistentAt))
                    .put("promotionId", dest.getPromotionId().toString())
                    .put("primaryAt", formatTimestamp(dest.getTransitionedTimestamp(PRIMARY)))
                    .build());
            assertEquals(src.getUuidString(), srcUuid);
            assertEquals(src.getState(), MIRROR_DEMOTED);
            assertNull(src.getMirrorExpiresAt());  // Not set yet
            assertEquals(src.getRawJson(), ImmutableMap.<String, Object>builder()
                    .put("placement", PL_US)
                    .put("shards", 16)
                    .put("moveTo", destUuid) // The old moveTo attribute are ignored since this is a mirror, but kept to facilitate debugging.
                    .build());
            assertBetween(start, TimeUUIDs.getDate(promotionId).toInstant(), end);
            assertBetween(start, primaryAt, end);

            // Verify that read/write mirroring is going to the right place.
            AstyanaxTable astyanaxTable = (AstyanaxTable) tableDAO.tableFromJson(table);
            assertTrue(astyanaxTable.getReadStorage().hasUUID(dest.getUuid()));
            assertStorageUuids(astyanaxTable.getWriteStorage(), dest.getUuid(), src.getUuid());

            // Verify that the next step in the move was scheduled as expected.
            MaintenanceOp maintenanceOp = tableDAO.getNextMaintenanceOp(TABLE);
            assertMaintenance(maintenanceOp, "Move:set-expiration", MaintenanceType.METADATA, "<system>");
            assertBetween(start, maintenanceOp.getWhen(), end, AstyanaxTableDAO.MIN_DELAY);
        }

        // Do maintenance from the US.  It should set the expiration timestamp on the old primary.
        Instant mirrorExpiresAt;
        {
            // Hack the table JSON to pretend the move copy finished long enough ago that now it's time to do the flip.
            primaryAt = primaryAt.minus(AstyanaxTableDAO.MIN_DELAY);
            patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.primaryAt", destUuid), primaryAt);

            Instant start = Instant.now();
            tableDAO.performMetadataMaintenance(TABLE);
            Instant end = Instant.now();

            table = tableDAO.readTableJson(TABLE, true);

            // Verify top-level attributes.
            assertEquals(table.getUuidString(), destUuid);
            assertEquals(table.getAttributeMap(), ImmutableMap.<String, Object>of());
            assertEquals(table.getStorages().size(), 2);

            // Verify storage-level attributes.
            Storage dest = checkNotNull(table.getMasterStorage());
            Storage src = Iterables.getOnlyElement(dest.getMirrors());
            mirrorExpiresAt = checkNotNull(src.getMirrorExpiresAt());
            assertEquals(dest.getUuidString(), destUuid);
            assertEquals(dest.getState(), PRIMARY);
            assertEquals(dest.getRawJson(), ImmutableMap.<String, Object>builder()
                    .put("placement", PL_GLOBAL)
                    .put("shards", 16)
                    .put("groupId", srcUuid)
                    .put("mirrorCreatedAt", formatTimestamp(mirrorCreatedAt))
                    .put("mirrorActivatedAt", formatTimestamp(mirrorActivatedAt))
                    .put("mirrorCopiedAt", formatTimestamp(mirrorCopiedAt))
                    .put("mirrorConsistentAt", formatTimestamp(mirrorConsistentAt))
                    .put("promotionId", dest.getPromotionId().toString())
                    .put("primaryAt", formatTimestamp(dest.getTransitionedTimestamp(PRIMARY)))
                    .build());
            assertEquals(src.getUuidString(), srcUuid);
            assertEquals(src.getState(), MIRROR_EXPIRING);
            assertBetween(start, src.getMirrorExpiresAt(), end, AstyanaxTableDAO.MOVE_DEMOTE_TO_EXPIRE);
            assertEquals(src.getRawJson(), ImmutableMap.<String, Object>builder()
                    .put("placement", PL_US)
                    .put("shards", 16)
                    .put("moveTo", destUuid)
                    .put("mirrorExpiresAt", formatTimestamp(mirrorExpiresAt))
                    .build());

            // Verify that read/write mirroring is going to the right place.
            AstyanaxTable astyanaxTable = (AstyanaxTable) tableDAO.tableFromJson(table);
            assertTrue(astyanaxTable.getReadStorage().hasUUID(dest.getUuid()));
            assertStorageUuids(astyanaxTable.getWriteStorage(), dest.getUuid(), src.getUuid());

            // Verify that the table events are correct
            assertTableEventsPresent(backingStore, ImmutableSet.of(DC_EU, DC_SA, DC_ZZ), TABLE, destUuid, PROMOTE);
            assertTableEventsAbsent(backingStore, ImmutableSet.of(DC_US, DC_EU, DC_SA, DC_ZZ), TABLE, srcUuid);
            assertTableEventsAbsent(backingStore, ImmutableSet.of(DC_US), TABLE, destUuid);

            // Hack the table event json to allow the table events to be marked ready
            Instant tableEventMaxTimestamp = tableDAO.getTableEvents(TABLE, destUuid, tableDAO.getTableEventDatacenters())
                    .map(Map.Entry::getValue)
                    .map(TableEvent::getEventTime)
                    .map(TimeUUIDs::getTimeMillis)
                    .max(Long::compare)
                    .map(Instant::ofEpochMilli)
                    .get();

            Instant expectedPrimaryMaintanenceTime = tableEventMaxTimestamp.plus(AstyanaxTableDAO.MIN_CONSISTENCY_DELAY);

            // Verify that the next step in the move was scheduled as expected.
            MaintenanceOp maintenanceOp = tableDAO.getNextMaintenanceOp(TABLE);
            assertMaintenance(maintenanceOp, "TableEvent:mark-ready", MaintenanceType.METADATA, "<system>");
            assertEquals(maintenanceOp.getWhen(), expectedPrimaryMaintanenceTime);

            patchTableEventJsonTimestamp(backingStore, TABLE, DC_EU, ImmutableSet.of(DC_EU), tableEventMaxTimestamp.minus(AstyanaxTableDAO.MIN_CONSISTENCY_DELAY));
            patchTableEventJsonTimestamp(backingStore, TABLE, DC_SA, ImmutableSet.of(DC_SA), tableEventMaxTimestamp.minus(AstyanaxTableDAO.MIN_CONSISTENCY_DELAY));
            patchTableEventJsonTimestamp(backingStore, TABLE, DC_ZZ, ImmutableSet.of(DC_ZZ), tableEventMaxTimestamp.minus(AstyanaxTableDAO.MIN_CONSISTENCY_DELAY));

            try {
                tableDAO.performMetadataMaintenance(TABLE);
                fail();
            } catch (PendingTableEventsException e) {

            }

            assertReadyTableEventPresent(backingStore, DC_EU, TABLE, destUuid, PROMOTE);
            assertReadyTableEventPresent(backingStore, DC_SA, TABLE, destUuid, PROMOTE);
            assertReadyTableEventPresent(backingStore, DC_ZZ, TABLE, destUuid, PROMOTE);
            assertReadyTableEventAbsent(backingStore, DC_US);
            assertReadyTableEventAbsent(backingStore, DC_EU);
            assertReadyTableEventAbsent(backingStore, DC_SA);
            assertReadyTableEventAbsent(backingStore, DC_ZZ);

            maintenanceOp = tableDAO.getNextMaintenanceOp(TABLE);
            assertMaintenance(maintenanceOp, "Move:expire-mirror", MaintenanceType.METADATA, "<system>");
            assertEquals(maintenanceOp.getWhen(), mirrorExpiresAt);

        }

        // Do maintenance when the mirror expires.  It should drop the src table uuid.
        Instant mirrorExpiredAt, droppedAt;
        {
            // Hack the table JSON to pretend the move copy finished long enough ago that now it's time to do the flip.
            mirrorExpiresAt = mirrorExpiresAt.minus(AstyanaxTableDAO.MOVE_DEMOTE_TO_EXPIRE);
            patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.mirrorExpiresAt", srcUuid), mirrorExpiresAt);

            Instant start = Instant.now();
            tableDAO.performMetadataMaintenance(TABLE);
            Instant end = Instant.now();

            table = tableDAO.readTableJson(TABLE, true);
            AstyanaxTable astyanaxTable = (AstyanaxTable) tableDAO.tableFromJson(table);

            // Verify top-level attributes.
            assertEquals(table.getUuidString(), destUuid);
            assertEquals(table.getAttributeMap(), ImmutableMap.<String, Object>of());
            assertEquals(table.getStorages().size(), 2);

            // Verify storage-level attributes.
            Storage dest = checkNotNull(table.getMasterStorage());
            Storage src = getStorage(table, srcUuid);
            mirrorExpiredAt = checkNotNull(src.getTransitionedTimestamp(MIRROR_EXPIRED));
            droppedAt = checkNotNull(src.getTransitionedTimestamp(DROPPED));
            assertEquals(dest.getUuidString(), destUuid);
            assertEquals(dest.getRawJson(), ImmutableMap.<String, Object>builder()
                    .put("placement", PL_GLOBAL)
                    .put("shards", 16)
                    .put("groupId", srcUuid)
                    .put("mirrorCreatedAt", formatTimestamp(mirrorCreatedAt))
                    .put("mirrorActivatedAt", formatTimestamp(mirrorActivatedAt))
                    .put("mirrorCopiedAt", formatTimestamp(mirrorCopiedAt))
                    .put("mirrorConsistentAt", formatTimestamp(mirrorConsistentAt))
                    .put("promotionId", dest.getPromotionId().toString())
                    .put("primaryAt", formatTimestamp(dest.getTransitionedTimestamp(PRIMARY)))
                    .build());
            assertTrue(src.isDropped());
            assertEquals(src.getUuidString(), srcUuid);
            assertEquals(src.getRawJson(), ImmutableMap.<String, Object>builder()
                    .put("placement", PL_US)
                    .put("shards", 16)
                    .put("moveTo", destUuid)
                    .put("mirrorExpiresAt", formatTimestamp(mirrorExpiresAt))
                    .put("mirrorExpiredAt", formatTimestamp(mirrorExpiredAt))
                    .put("droppedAt", formatTimestamp(droppedAt))
                    .build());
            assertBetween(start, droppedAt, end);

            // Verify that read/write mirroring is turned off.
            assertTrue(astyanaxTable.getReadStorage().hasUUID(dest.getUuid()));
            assertStorageUuids(astyanaxTable.getWriteStorage(), dest.getUuid());

            // Verify that the next step in the move was scheduled as expected.
            MaintenanceOp maintenanceOp = tableDAO.getNextMaintenanceOp(TABLE);
            assertMaintenance(maintenanceOp, "Drop:purge-1", MaintenanceType.DATA, DC_US);
            assertBetween(start, maintenanceOp.getWhen(), end, AstyanaxTableDAO.DROP_TO_PURGE_1);
        }
    }

    @Test
    public void testMoveCanceledBeforePromote()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.move(TABLE, PL_GLOBAL, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);
        TableJson table = tableDAO.readTableJson(TABLE, true);

        try {
            tableDAO.move(TABLE, PL_US, Optional.of(16), newAudit(), MoveType.SINGLE_TABLE);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "This table name is currently undergoing maintenance and therefore cannot be modified: my:table");
        }
    }

    @Test
    public void testMoveCanceledAfterPromote()
            throws Exception {
        InMemoryDataStore backingStore = newBackingStore(new MetricRegistry());
        Date fct = new Date(0);
        AstyanaxTableDAO tableDAO = newTableDAO(backingStore, DC_US, mock(DataCopyDAO.class), mock(DataPurgeDAO.class), fct);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());

        // Perform an initial move.
        tableDAO.move(TABLE, PL_GLOBAL, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);

        TableJson table = tableDAO.readTableJson(TABLE, true);
        String srcUuid = checkNotNull(table.getUuidString());
        String destUuid = checkNotNull(table.getMasterStorage().getMoveTo().getUuidString());

        // Hack the table JSON to get to the state where promote has occurred.
        advanceActivatedToPromoted(destUuid, tableDAO, backingStore, fct);

        try {
            tableDAO.move(TABLE, PL_US, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "This table name is currently undergoing maintenance and therefore cannot be modified: my:table");
        }
    }

    @Test
    public void testMovePlacement()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        PlacementFactory placementFactory = newPlacementFactory(DC_US);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.create(TABLE1, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.create(TABLE2, newOptions(PL_EU), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.create(TABLE3, newOptions(PL_GLOBAL), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.create(TABLE4, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.create(TABLE5, newOptions(PL_EU), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.create(TABLE6, newOptions(PL_EU), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_EU), newAudit());
        tableDAO.createFacade(TABLE1, newFacadeOptions(PL_EU), newAudit());
        tableDAO.createFacade(TABLE2, newFacadeOptions(PL_US), newAudit());

        // Start moving a table into another placement that should not affect this placement move
        tableDAO.move(TABLE5, PL_GLOBAL, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);

        MoveTableTask moveTableTask =
                new MoveTableTask(mock(TaskRegistry.class), "blob", tableDAO, mock(MaintenanceDAO.class),
                        ImmutableMap.of(PL_ZZ_MOVING, PL_ZZ));
        MoveTableTask.MovePlacement movePlacement = moveTableTask.getTablesAndFacadesInSrcPlacement(PL_US);

        assertEquals(movePlacement.getTables().size(), 3, "There should be 3 tables");
        assertTrue(movePlacement.getTables().contains(TABLE), format("%s should be included", TABLE));
        assertTrue(movePlacement.getTables().contains(TABLE1), format("%s should be included", TABLE1));
        assertTrue(movePlacement.getTables().contains(TABLE4), format("%s should be included", TABLE4));

        assertEquals(movePlacement.getFacades().size(), 1, "There should be 1 facade");
        assertTrue(movePlacement.getFacades().contains(TABLE2), format("%s facade should be included", TABLE2));

        // Move placement that is not accessible from System DataCenter.
        movePlacement = moveTableTask.getTablesAndFacadesInSrcPlacement(PL_EU);
        assertEquals(movePlacement.getTables().size(), 3, "There should be 3 tables");
        assertTrue(movePlacement.getTables().contains(TABLE2), format("%s should be included", TABLE2));
        assertTrue(movePlacement.getTables().contains(TABLE5), format("%s should be included", TABLE5));
        assertTrue(movePlacement.getTables().contains(TABLE6), format("%s should be included", TABLE6));

        assertEquals(movePlacement.getFacades().size(), 2, "There should be 2 facades");
        assertTrue(movePlacement.getFacades().contains(TABLE), format("%s facade should be included", TABLE));
        assertTrue(movePlacement.getFacades().contains(TABLE1), format("%s facade should be included", TABLE1));
    }

    @Test (expectedExceptions = IllegalStateException.class)
    public void testMovePlacementWhileTableIsGettingMovedIntoSource()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        PlacementFactory placementFactory = newPlacementFactory(DC_US);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.create(TABLE1, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.create(TABLE2, newOptions(PL_EU), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.create(TABLE3, newOptions(PL_GLOBAL), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.create(TABLE4, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_EU), newAudit());
        tableDAO.createFacade(TABLE1, newFacadeOptions(PL_EU), newAudit());
        tableDAO.createFacade(TABLE2, newFacadeOptions(PL_US), newAudit());

        // Start moving a table into placement that we are trying to move
        tableDAO.move(TABLE3, PL_US, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);

        MoveTableTask moveTableTask =
                new MoveTableTask(mock(TaskRegistry.class), "blob", tableDAO, mock(MaintenanceDAO.class), ImmutableMap.of(PL_ZZ_MOVING, PL_ZZ));

        // This should throw an IllegalStateException due to the move above
        MoveTableTask.MovePlacement movePlacement = moveTableTask.getTablesAndFacadesInSrcPlacement(PL_US);
    }

    /**
     * Test move() then move() again before the first move() finishes.  Similar to testMoveCanceledBeforePromote()
     * which tests move() then move() back except this test involves a 3rd storage.
     */
    @Test
    public void testMoveChangedBeforePromotion()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.move(TABLE, PL_GLOBAL, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);
        TableJson table = tableDAO.readTableJson(TABLE, true);
        String srcUuid = checkNotNull(table.getUuidString());
        String destUuid = checkNotNull(table.getMasterStorage().getMoveTo().getUuidString());

        try {
            tableDAO.move(TABLE, PL_US, Optional.of(32), newAudit(), MoveType.SINGLE_TABLE);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "This table name is currently undergoing maintenance and therefore cannot be modified: my:table");
        }
    }


    @Test
    public void testIfPurge1IsSkippedUnderPlacementWideMove()
            throws Exception {
        Purge1IsSkipped(MoveType.FULL_PLACEMENT); // PURGED_1 should be skipped. After a day is passed, it should stay at DROPPED.
        Purge1IsSkipped(MoveType.SINGLE_TABLE); // PURGED_1 should not be skipped. After a day is passed, it should move to PURGED_1.
    }

    private void Purge1IsSkipped(MoveType moveType)
            throws Exception {
        InMemoryDataStore backingStore = newBackingStore(new MetricRegistry());
        Date fct = new Date(0);
        AstyanaxTableDAO tableDAO = newTableDAO(backingStore, DC_US, mock(DataCopyDAO.class), mock(DataPurgeDAO.class), fct);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());

        // Perform an initial move as a part of placement move (the only difference is it skips the PURGE_1 state)
        tableDAO.move(TABLE, PL_GLOBAL, Optional.<Integer>absent(), newAudit(), moveType);

        TableJson table = tableDAO.readTableJson(TABLE, true);
        String srcUuid = checkNotNull(table.getUuidString());
        String destUuid = checkNotNull(table.getMasterStorage().getMoveTo().getUuidString());

        // Hack the table JSON to get to the state where promote has occurred.
        advanceActivatedToSourcePurged(destUuid, tableDAO, backingStore, fct);

        // Make sure PURGED_1 is skipped
        if (moveType == MoveType.FULL_PLACEMENT) {
            assertEquals(getStorage(tableDAO.readTableJson(TABLE, true), srcUuid).getState(), DROPPED);
        } else {
            assertEquals(getStorage(tableDAO.readTableJson(TABLE, true), srcUuid).getState(), PURGED_1);
        }
    }

    @Test
    public void testCreatesAreMadeInNewPlacementIfGivenPlacementUnderMove()
            throws Exception {
        InMemoryDataStore backingStore = newBackingStore(new MetricRegistry());
        Date fct = new Date(0);
        AstyanaxTableDAO tableDAO = newTableDAO(backingStore, DC_US, mock(DataCopyDAO.class), mock(DataPurgeDAO.class), fct);
        tableDAO.create(TABLE, newOptions(PL_ZZ_MOVING), ImmutableMap.<String, Object>of(), newAudit());
        // Make sure this table is created in
        assertEquals(tableDAO.get(TABLE).getOptions().getPlacement(), PL_ZZ,
                "Placement should revert to the new placement if it is under move.");
        assertFalse(tableDAO.isMoveToThisPlacementAllowed(PL_ZZ_MOVING), "PL_ZZ_MOVING");
    }

    @Test
    public void testMoveChangedAfterPromotion()
            throws Exception {
        InMemoryDataStore backingStore = newBackingStore(new MetricRegistry());
        Date fct = new Date(0);
        AstyanaxTableDAO tableDAO = newTableDAO(backingStore, DC_US, mock(DataCopyDAO.class), mock(DataPurgeDAO.class), fct);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());

        // Perform an initial move
        tableDAO.move(TABLE, PL_GLOBAL, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);

        TableJson table = tableDAO.readTableJson(TABLE, true);
        String srcUuid = checkNotNull(table.getUuidString());
        String destUuid = checkNotNull(table.getMasterStorage().getMoveTo().getUuidString());

        // Hack the table JSON to get to the state where promote has occurred.
        advanceActivatedToPromoted(destUuid, tableDAO, backingStore, fct);

        try {
            tableDAO.move(TABLE, PL_US, Optional.of(32), newAudit(), MoveType.SINGLE_TABLE);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "This table name is currently undergoing maintenance and therefore cannot be modified: my:table");
        }
    }

    /**
     * Can't move a facade directly from us to eu since there are no data centers in common.
     */
    @Test
    public void testMoveFacadeIncompatiblePlacement()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());

        tableDAO.createFacade(TABLE, newFacadeOptions(PL_EU), newAudit());
        try {
            tableDAO.move(TABLE, PL_APAC, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Source and destination and mirror placements must overlap in some data center: apac, us");
        }
    }

    /**
     * Moving a table may cause a facade to become inaccessible.
     */
    @Test
    public void testMoveOverlapsFacade()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_EU), newAudit());
        tableDAO.move(TABLE, PL_GLOBAL, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);
    }

    /**
     * Moving a facade may not cause the facade to become inaccessible
     */
    @Test
    public void testMoveFacadeOverlapsMaster()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_GLOBAL), newAudit());

        try {
            tableDAO.moveFacade(TABLE, PL_GLOBAL, PL_US, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "Cannot create a facade in the same placement as its table: us");
        }
    }

    /**
     * Moving a facade may not cause the choice of facade to become ambiguous in some data center.
     */
    @Test
    public void testMoveFacadeOverlapsFacade()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_APAC), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_US), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_EU), newAudit());

        try {
            tableDAO.moveFacade(TABLE, PL_EU, PL_GLOBAL, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);
            fail();
        } catch (FacadeExistsException e) {
            assertEquals(e.getMessage(), "Cannot create a facade in this placement as it will overlap with other facade placements: my:table (on us)");
            assertEquals(e.getTable(), TABLE);
            assertEquals(e.getPlacement(), PL_US);
        }
    }

    /**
     * Creating a facade checks existing against both source and destination of table moves-in-progress.
     */
    @Test
    public void testCreateFacadeOverlapsMove()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_APAC), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.move(TABLE, PL_GLOBAL, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);

        // Move hasn't completed yet, can't create a facade in the old location.
        try {
            tableDAO.createFacade(TABLE, newFacadeOptions(PL_APAC), newAudit());
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "This table name is currently undergoing maintenance and therefore cannot be modified: my:table");
        }
        // And can't create a facade that overlaps with the new location.
        try {
            tableDAO.createFacade(TABLE, newFacadeOptions(PL_GLOBAL), newAudit());
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "This table name is currently undergoing maintenance and therefore cannot be modified: my:table");
        }
    }

    /**
     * Creating a facade checks existing against both source and destination of facade moves-in-progress.
     */
    @Test
    public void testCreateFacadeOverlapsMoveFacade()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_APAC), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_EU), newAudit());
        tableDAO.moveFacade(TABLE, PL_EU, PL_GLOBAL, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);

        // Move hasn't completed yet, creating a facade anywhere isn't allowed
        try {
            tableDAO.createFacade(TABLE, newFacadeOptions(PL_EU), newAudit());
            fail();
        } catch (IllegalArgumentException e)
        {
            assertEquals(e.getMessage(), "This table name is currently undergoing maintenance and therefore cannot be modified: my:table");
        }

        // Move hasn't completed yet, can't create a facade in the new location
        try {
            tableDAO.createFacade(TABLE, newFacadeOptions(PL_GLOBAL), newAudit());
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "This table name is currently undergoing maintenance and therefore cannot be modified: my:table");
        }
        // And can't create a facade that overlaps with the new location.
        try {
            tableDAO.createFacade(TABLE, newFacadeOptions(PL_US), newAudit());
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "This table name is currently undergoing maintenance and therefore cannot be modified: my:table");
        }
    }

    /**
     * Moving a facade checks existing against the source of facade moves-in-progress.
     */
    @Test
    public void testMoveFacadeOverlapsMoveFacadeSource()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_EU), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_US_EU), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_APAC), newAudit());
        tableDAO.moveFacade(TABLE, PL_US_EU, PL_US, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);

        // Move hasn't completed yet, moving a facade to the old location is not allowed.
        try {
            tableDAO.moveFacade(TABLE, PL_APAC, PL_US_EU, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "This table name is currently undergoing maintenance and therefore cannot be modified: my:table");
        }
    }

    /**
     * Moving a facade checks existing against the destination of facade moves-in-progress.
     */
    @Test
    public void testMoveFacadeOverlapsMoveFacadeDestination()
            throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US);
        tableDAO.create(TABLE, newOptions(PL_ZZ), ImmutableMap.<String, Object>of(), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_US), newAudit());
        tableDAO.createFacade(TABLE, newFacadeOptions(PL_APAC), newAudit());
        tableDAO.moveFacade(TABLE, PL_US, PL_US_EU, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);

        // Move hasn't completed yet, moving a facade to the new location is not allowed.
        try {
            tableDAO.moveFacade(TABLE, PL_APAC, PL_EU_APAC, Optional.<Integer>absent(), newAudit(), MoveType.SINGLE_TABLE);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "This table name is currently undergoing maintenance and therefore cannot be modified: my:table");
        }
    }

    @Test
    public void testListUnpublishedDatabusEvents()
            throws Exception {
        Instant from = Instant.now();
        Instant to = Instant.now().plus(Duration.ofDays(1));

        final Clock clock = mock(Clock.class);
        Instant nowInstant = from.plus(Duration.ofMinutes(1)); // adding a second just to make sure that the delta was written after the "from" time.
        when(clock.instant()).thenReturn(nowInstant);
        InMemoryDataStore backingStore = newBackingStore(new MetricRegistry());
        Date fct = new Date(0);
        AstyanaxTableDAO tableDAO = newTableDAO(backingStore, DC_US, clock, fct);

        // create a table.
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of("space", "test"), newAudit());


        Iterator<Table> tableIterator = tableDAO.list(null, LimitCounter.max());
        assertTrue(tableIterator.hasNext());

        // drop a table.
        tableDAO.drop(TABLE, newAudit());

        // unpublished databus events should have the dropped table.
        Iterator<UnpublishedDatabusEvent> unpublishedDatabusEventsIterator = tableDAO.listUnpublishedDatabusEvents(Date.from(from), Date.from(to));
        assertTrue(unpublishedDatabusEventsIterator.hasNext());
        UnpublishedDatabusEvent unpublishedDatabusEvent = unpublishedDatabusEventsIterator.next();
        assertTrue(unpublishedDatabusEvent.getTable().equals(TABLE));
        assertTrue(unpublishedDatabusEvent.getEventType().equals(UnpublishedDatabusEventType.DROP_TABLE));
        assertEquals(unpublishedDatabusEvent.getDate(), Date.from(nowInstant));

        // create a second table
        tableDAO.create(TABLE2, newOptions(PL_US), ImmutableMap.<String, Object>of("space", "test"), newAudit());

        String table2Uuid = checkNotNull(tableDAO.readTableJson(TABLE2, true).getUuidString());

        // drop the second table
        tableDAO.drop(TABLE2, newAudit());

        // unpublished databus events should now have 2 tables.
        Iterator<UnpublishedDatabusEvent> unpublishedDatabusEventsIterator2 = tableDAO.listUnpublishedDatabusEvents(Date.from(from), Date.from(to));
        assertTrue(unpublishedDatabusEventsIterator2.hasNext());
        UnpublishedDatabusEvent unpublishedDatabusEvent2 = unpublishedDatabusEventsIterator2.next();
        UnpublishedDatabusEvent unpublishedDatabusEvent3 = unpublishedDatabusEventsIterator2.next();
        List<String> expectedTables = Lists.newArrayList(unpublishedDatabusEvent2.getTable(), unpublishedDatabusEvent3.getTable());
        Assertions.assertThat(expectedTables).containsOnly(TABLE, TABLE2);

        // Hack the table maintanence process to complete purge and final deletion
        {
            Instant droppedAt = getStorage(tableDAO.readTableJson(TABLE2, false), table2Uuid)
                    .getTransitionedTimestamp(DROPPED);

            droppedAt = droppedAt.minus(AstyanaxTableDAO.DROP_TO_PURGE_2);
            patchTableJsonTimestamp(backingStore, TABLE2, format("storage.%s.droppedAt", table2Uuid), droppedAt);

            fct.setTime(droppedAt.toEpochMilli() + 1);
            tableDAO.performDataMaintenance(TABLE2, mock(Runnable.class));

            Instant purgedAt = getStorage(tableDAO.readTableJson(TABLE2, false), table2Uuid)
                    .getTransitionedTimestamp(PURGED_2);
            purgedAt = purgedAt.minus(AstyanaxTableDAO.MIN_CONSISTENCY_DELAY);

            patchTableJsonTimestamp(backingStore, TABLE2, format("storage.%s.purgedAt2", table2Uuid), purgedAt);

            tableDAO.performMetadataMaintenance(TABLE2);
        }

        // recreate the second table.
        tableDAO.create(TABLE2, newOptions(PL_US), ImmutableMap.<String, Object>of("space", "test"), newAudit());


        // unpublished databus events still should have 2 tables.
        Iterator<UnpublishedDatabusEvent> unpublishedDatabusEventsIterator3 = tableDAO.listUnpublishedDatabusEvents(Date.from(from), Date.from(to));
        assertTrue(unpublishedDatabusEventsIterator3.hasNext());
        UnpublishedDatabusEvent unpublishedDatabusEvent4 = unpublishedDatabusEventsIterator3.next();
        UnpublishedDatabusEvent unpublishedDatabusEvent5 = unpublishedDatabusEventsIterator3.next();
        List<String> expectedTables2 = Lists.newArrayList(unpublishedDatabusEvent4.getTable(), unpublishedDatabusEvent5.getTable());
        Assertions.assertThat(expectedTables2).containsOnly(TABLE, TABLE2);

        // create a third table.
        tableDAO.create(TABLE3, newOptions(PL_US), ImmutableMap.<String, Object>of("space", "test"), newAudit());

        // update attributes to the table
        tableDAO.setAttributes(TABLE3, ImmutableMap.<String, Object>of("new-key", "new-value"), newAudit());

        // unpublished databus events should now have 3 tables.
        Iterator<UnpublishedDatabusEvent> unpublishedDatabusEventsIterator4 = tableDAO.listUnpublishedDatabusEvents(Date.from(from), Date.from(to));
        assertTrue(unpublishedDatabusEventsIterator4.hasNext());
        UnpublishedDatabusEvent unpublishedDatabusEvent6 = unpublishedDatabusEventsIterator4.next();
        assertTrue(unpublishedDatabusEventsIterator4.hasNext());
        UnpublishedDatabusEvent unpublishedDatabusEvent7 = unpublishedDatabusEventsIterator4.next();
        assertTrue(unpublishedDatabusEventsIterator4.hasNext());
        UnpublishedDatabusEvent unpublishedDatabusEvent8 = unpublishedDatabusEventsIterator4.next();
        List<String> expectedTables4 = Lists.newArrayList(unpublishedDatabusEvent6.getTable(), unpublishedDatabusEvent7.getTable(), unpublishedDatabusEvent8.getTable());
        Assertions.assertThat(expectedTables4).containsOnly(TABLE, TABLE2, TABLE3);
        assertTrue(unpublishedDatabusEvent6.getEventType().equals(UnpublishedDatabusEventType.DROP_TABLE));
        assertTrue(unpublishedDatabusEvent7.getEventType().equals(UnpublishedDatabusEventType.DROP_TABLE));
        assertTrue(unpublishedDatabusEvent8.getEventType().equals(UnpublishedDatabusEventType.UPDATE_ATTRIBUTES));
    }

    @Test
    public void testListUnpublishedDatabusEventsWithEventBeforeSpecifiedFromDate()
            throws Exception {
        Instant from = Instant.now();
        Instant to = Instant.now().plus(Duration.ofDays(1));

        final Clock clock = mock(Clock.class);
        Instant nowInstant = from.minus(Duration.ofMinutes(1)); // make sure the drop event is before the specified "from" time.
        when(clock.instant()).thenReturn(nowInstant);
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US, clock);

        // create a table.
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of("space", "test"), newAudit());

        Iterator<Table> tableIterator = tableDAO.list(null, LimitCounter.max());
        assertTrue(tableIterator.hasNext());

        // drop a table.
        tableDAO.drop(TABLE, newAudit());

        // unpublished databus events should NOT have the dropped table.
        Iterator<UnpublishedDatabusEvent> unpublishedDatabusEventsIterator = tableDAO.listUnpublishedDatabusEvents(Date.from(from), Date.from(to));
        assertFalse(unpublishedDatabusEventsIterator.hasNext());
    }

    @Test
    public void testListUnpublishedDatabusEventsWithDifferentTimesOnSameDay()
            throws Exception {
        Instant now = Instant.now();

        final Clock clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US, clock);

        Instant from = now.minus(Duration.ofMinutes(10));
        Instant to = now.plus(Duration.ofMinutes(10));

        // create a table.
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of("space", "test"), newAudit());

        Iterator<Table> tableIterator = tableDAO.list(null, LimitCounter.max());
        assertTrue(tableIterator.hasNext());

        // drop a table.
        tableDAO.drop(TABLE, newAudit());

        // unpublished databus events should have the dropped table.
        Iterator<UnpublishedDatabusEvent> unpublishedDatabusEventsIterator = tableDAO.listUnpublishedDatabusEvents(Date.from(from), Date.from(to));
        assertTrue(unpublishedDatabusEventsIterator.hasNext());
        UnpublishedDatabusEvent unpublishedDatabusEvent = unpublishedDatabusEventsIterator.next();
        assertTrue(unpublishedDatabusEvent.getTable().equals(TABLE));

        // changing to to fall before the drop event time.
        to = now.minus(Duration.ofMinutes(1));

        // unpublished databus events should NOT have the dropped table.
        Iterator<UnpublishedDatabusEvent> unpublishedDatabusEventsIterator2 = tableDAO.listUnpublishedDatabusEvents(Date.from(from), Date.from(to));
        assertFalse(unpublishedDatabusEventsIterator2.hasNext());
    }

    @Test
    public void testListUnpublishedDatabusEventsWithEventsOnDifferentDays()
            throws Exception {
        Instant from = Instant.now();
        Instant to = Instant.now().plus(Duration.ofDays(1));

        final Clock clock = mock(Clock.class);
        Instant nowInstant = from.plus(Duration.ofMinutes(1)); // adding a second just to make sure that the delta was written after the "from" time.
        when(clock.instant()).thenReturn(nowInstant);
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US, clock);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        // create a table.
        tableDAO.create(TABLE1, newOptions(PL_US), ImmutableMap.<String, Object>of("space", "test"), newAudit());

        // drop a table.
        tableDAO.drop(TABLE1, newAudit());

        // unpublished databus events should have the dropped table.
        Iterator<UnpublishedDatabusEvent> unpublishedDatabusEventsIterator = tableDAO.listUnpublishedDatabusEvents(Date.from(from), Date.from(to));
        assertTrue(unpublishedDatabusEventsIterator.hasNext());
        UnpublishedDatabusEvent unpublishedDatabusEvent = unpublishedDatabusEventsIterator.next();
        assertTrue(unpublishedDatabusEvent.getTable().equals(TABLE1));

        // Now set the clock is set to 1 days in advance.
        Instant nextDayInstant = nowInstant.plus(Duration.ofDays(1));
        when(clock.instant()).thenReturn(nextDayInstant);

        // create a second table
        tableDAO.create(TABLE2, newOptions(PL_US), ImmutableMap.<String, Object>of("space", "test"), newAudit());

        // drop the second table
        tableDAO.drop(TABLE2, newAudit());

        // unpublished databus events should only have TABLE1 information as the TABLE2 DROP was registered on the next day.
        Iterator<UnpublishedDatabusEvent> unpublishedDatabusEventsIterator2 = tableDAO.listUnpublishedDatabusEvents(Date.from(from), Date.from(to));
        assertTrue(unpublishedDatabusEventsIterator2.hasNext());
        UnpublishedDatabusEvent unpublishedDatabusEvent2 = unpublishedDatabusEventsIterator2.next();
        assertFalse(unpublishedDatabusEventsIterator2.hasNext());
        assertTrue(unpublishedDatabusEvent2.getTable().equals(TABLE1));
        assertEquals(unpublishedDatabusEvent2.getDate(), Date.from(nowInstant));

        // Now query for 2 days with "to" advanced by a day.
        // unpublished databus events should have "two" records, one for each day.
        // Each record should have one table information.
        Iterator<UnpublishedDatabusEvent> unpublishedDatabusEventsIterator3 = tableDAO.listUnpublishedDatabusEvents(Date.from(from), Date.from(to.plus(Duration.ofDays(1))));
        assertTrue(unpublishedDatabusEventsIterator3.hasNext());
        UnpublishedDatabusEvent unpublishedDatabusEvent3 = unpublishedDatabusEventsIterator3.next();
        assertTrue(unpublishedDatabusEventsIterator3.hasNext());
        UnpublishedDatabusEvent unpublishedDatabusEvent4 = unpublishedDatabusEventsIterator3.next();
        List<String> expectedTables = Lists.newArrayList(unpublishedDatabusEvent3.getTable(), unpublishedDatabusEvent4.getTable());
        List<Long> expectedDates = Lists.newArrayList(unpublishedDatabusEvent3.getDate().getTime(), unpublishedDatabusEvent4.getDate().getTime());
        Assertions.assertThat(expectedTables).containsOnly(TABLE1, TABLE2);
        assertTrue(expectedDates.contains(nowInstant.toEpochMilli()));
        assertTrue(expectedDates.contains(nextDayInstant.toEpochMilli()));
    }

    @Test
    public void testMillisecondPrecisionZonedDateTime()
            throws Exception {
        ZonedDateTime zeroSecondDateTime = ZonedDateTime.of(LocalDate.parse("2017-01-01"), LocalTime.parse("07:01"), ZoneOffset.UTC);
        ZonedDateTime zeroMilliSecondDateTime = ZonedDateTime.of(LocalDate.parse("2017-01-01"), LocalTime.parse("07:01:01"), ZoneOffset.UTC);

        assertEquals(zeroSecondDateTime.toString(), "2017-01-01T07:01Z");
        assertEquals(zeroMilliSecondDateTime.toString(), "2017-01-01T07:01:01Z");

        assertEquals(AstyanaxTableDAO.getMillisecondPrecisionZonedDateTime(zeroSecondDateTime), "2017-01-01T07:01:00.000Z");
        assertEquals(AstyanaxTableDAO.getMillisecondPrecisionZonedDateTime(zeroMilliSecondDateTime), "2017-01-01T07:01:01.000Z");
    }

    @Test
    public void testListUnpublishedDatabusEventsWhenDropHappensAtZeroSecondInstant()
            throws Exception {

        // Time with Zero Seconds.
        ZonedDateTime zeroSecondDateTime = ZonedDateTime.of(LocalDate.parse("2016-01-01"), LocalTime.parse("07:01"), ZoneOffset.UTC);

        final Clock clock = mock(Clock.class);
        when(clock.instant()).thenReturn(zeroSecondDateTime.toInstant());
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US, clock);

        Instant from = zeroSecondDateTime.toInstant().minus(Duration.ofHours(1));
        Instant to = zeroSecondDateTime.toInstant().plus(Duration.ofHours(1));

        // create a table.
        tableDAO.create(TABLE, newOptions(PL_US), ImmutableMap.<String, Object>of("space", "test"), newAudit());

        Iterator<Table> tableIterator = tableDAO.list(null, LimitCounter.max());
        assertTrue(tableIterator.hasNext());

        // drop a table.
        tableDAO.drop(TABLE, newAudit());

        // unpublished databus events should have the dropped table and with the specified time.
        // Without the getMillisecondPrecisionZonedDateTime format change, the below will throw a parsing error and this test will fail.
        Iterator<UnpublishedDatabusEvent> unpublishedDatabusEventsIterator = tableDAO.listUnpublishedDatabusEvents(Date.from(from), Date.from(to));
        assertTrue(unpublishedDatabusEventsIterator.hasNext());
        UnpublishedDatabusEvent unpublishedDatabusEvent = unpublishedDatabusEventsIterator.next();
        assertTrue(unpublishedDatabusEvent.getTable().equals(TABLE));
        assertTrue(unpublishedDatabusEvent.getEventType().equals(UnpublishedDatabusEventType.DROP_TABLE));
        assertEquals(unpublishedDatabusEvent.getDate(), Date.from(zeroSecondDateTime.toInstant()));
    }

    //
    // Helper methods
    //

    private void advanceActivatedToPromoted(String destUuid, AstyanaxTableDAO tableDAO, InMemoryDataStore backingStore, Date fct) {
        String srcUuid = tableDAO.readTableJson(TABLE, true).getMasterStorage().getUuidString();

        // MIRROR_ACTIVATED -> MIRROR_COPIED
        assertEquals(getStorage(tableDAO.readTableJson(TABLE, true), destUuid).getState(), MIRROR_ACTIVATED);
        patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.mirrorActivatedAt", destUuid),
                Instant.now().minus(AstyanaxTableDAO.MIN_CONSISTENCY_DELAY));
        fct.setTime(System.currentTimeMillis() + 1);
        tableDAO.performDataMaintenance(TABLE, mock(Runnable.class));

        // MIRROR_COPIED -> MIRROR_CONSISTENT
        assertEquals(getStorage(tableDAO.readTableJson(TABLE, true), destUuid).getState(), MIRROR_COPIED);
        patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.mirrorCopiedAt", destUuid),
                Instant.now().minus(AstyanaxTableDAO.MIN_CONSISTENCY_DELAY));
        fct.setTime(System.currentTimeMillis() + 1);
        tableDAO.performDataMaintenance(TABLE, mock(Runnable.class));

        // MIRROR_CONSISTENT -> PRIMARY
        assertEquals(getStorage(tableDAO.readTableJson(TABLE, true), destUuid).getState(), MIRROR_CONSISTENT);
        patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.mirrorConsistentAt", destUuid),
                Instant.now().minus(AstyanaxTableDAO.MIN_DELAY));
        tableDAO.performMetadataMaintenance(TABLE);

        // Make sure the promote happened.
        assertEquals(tableDAO.readTableJson(TABLE, true).getMasterStorage().getUuidString(), destUuid);

        // MIRROR_DEMOTED -> MIRROR_EXPIRING (for the old primary)
        patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.primaryAt", destUuid),
                Instant.now().minus(AstyanaxTableDAO.MIN_DELAY));
        tableDAO.performMetadataMaintenance(TABLE);

        // Check that all is in the expected final state.
        assertEquals(getStorage(tableDAO.readTableJson(TABLE, true), srcUuid).getState(), MIRROR_EXPIRING);
        assertEquals(getStorage(tableDAO.readTableJson(TABLE, true), destUuid).getState(), PRIMARY);
    }

    private void advanceActivatedToSourcePurged(String destUuid, AstyanaxTableDAO tableDAO, InMemoryDataStore backingStore, Date fct) {
        String srcUuid = tableDAO.readTableJson(TABLE, true).getMasterStorage().getUuidString();
        advanceActivatedToPromoted(destUuid, tableDAO, backingStore, fct);

        // MIRROR_EXPIRING -> DROPPED (for the old primary)
        patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.mirrorExpiresAt", srcUuid),
                Instant.now().minus(AstyanaxTableDAO.MOVE_DEMOTE_TO_EXPIRE));
        tableDAO.performMetadataMaintenance(TABLE);

        // DROPPED -> PURGED_1/PURGED_2 (for the old primary)
        assertEquals(getStorage(tableDAO.readTableJson(TABLE, true), srcUuid).getState(), DROPPED);
        patchTableJsonTimestamp(backingStore, TABLE, format("storage.%s.droppedAt", srcUuid),
                Instant.now().minus(AstyanaxTableDAO.DROP_TO_PURGE_1));
        tableDAO.performDataMaintenance(TABLE, mock(Runnable.class));
    }

    private void assertMaintenance(MaintenanceOp maintenanceOp, String name, MaintenanceType type, String dataCenter) {
        assertEquals(maintenanceOp.getName(), name);
        assertEquals(maintenanceOp.getType(), type);
        assertEquals(maintenanceOp.getDataCenter(), dataCenter);
    }

    private void assertNoopMetadataMaintenance(InMemoryDataStore backingStore, String dataCenter, String table)
            throws Exception {
        DataCopyDAO dataCopyDAO = mock(DataCopyDAO.class);
        DataPurgeDAO dataPurgeDAO = mock(DataPurgeDAO.class);
        AstyanaxTableDAO tableDAO = newTableDAO(backingStore, dataCenter, dataCopyDAO, dataPurgeDAO, new Date(0));
        TableJson before = tableDAO.readTableJson(table, false);

        tableDAO.performMetadataMaintenance(table);

        TableJson after = tableDAO.readTableJson(table, false);
        assertEquals(after.getRawJson(), before.getRawJson());
        verifyNoMoreInteractions(dataCopyDAO, dataPurgeDAO);
    }

    private void assertNoopDataMaintenance(InMemoryDataStore backingStore, String dataCenter, String table)
            throws Exception {
        DataCopyDAO euDataCopyDAO = mock(DataCopyDAO.class);
        DataPurgeDAO euDataPurgeDAO = mock(DataPurgeDAO.class);
        AstyanaxTableDAO euTableDAO = newTableDAO(backingStore, dataCenter, euDataCopyDAO, euDataPurgeDAO, new Date(0));

        euTableDAO.performDataMaintenance(table, mock(Runnable.class));

        // Wrong data center.  Nothing should have happened.
        verifyNoMoreInteractions(euDataCopyDAO, euDataPurgeDAO);
    }

    private void assertBetween(Instant start, Instant actual, Instant end) {
        assertBetween(start, actual, end, Duration.ZERO);
    }

    private void assertBetween(Instant start, Instant actual, Instant end, Duration offset) {
        assertTrue(actual.compareTo(start.plus(offset)) >= 0, JsonHelper.formatTimestamp(actual.toEpochMilli()));
        assertTrue(actual.compareTo(end.plus(offset)) <= 0, JsonHelper.formatTimestamp(actual.toEpochMilli()));
    }

    private void assertStorageUuids(Collection<AstyanaxStorage> storages, long... uuids) {
        Set<Long> expected = Sets.newHashSet();
        for (long uuid : uuids) {
            expected.add(uuid);
        }
        for (AstyanaxStorage storage : storages) {
            boolean found = false;
            for (long uuid : expected) {
                if (storage.hasUUID(uuid)) {
                    expected.remove(uuid);
                    found = true;
                    break;
                }
            }
            assertTrue(found);
        }
        assertTrue(expected.isEmpty());
    }

    private void assertReadyTableEventPresent(TableBackingStore backingStore, String datacenter, String table, String uuid,
                                              TableEvent.Action action) throws Exception {
        TableEventRegistry tableEventRegistry = newTableDAO(datacenter, backingStore);
        Map.Entry<String, TableEvent> tableEvent = tableEventRegistry.getNextReadyTableEvent(datacenter);

        assertEquals(tableEvent.getKey(), TABLE);
        assertEquals(tableEvent.getValue().getAction(), action);
        assertEquals(tableEvent.getValue().getStorage(), uuid);

        tableEventRegistry.markTableEventAsComplete(datacenter, tableEvent.getKey(), tableEvent.getValue().getStorage());
    }

    private void assertTableEventsPresent(TableBackingStore backingStore, Set<String> datacenters, String table,
                                          String uuid, TableEvent.Action action) throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US, backingStore);
        Set<String> tableEventDatacenters = tableDAO.getTableEvents(table, uuid, tableDAO.getTableEventDatacenters())
                .peek(entry -> assertEquals(entry.getValue().getAction(), action))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        assertEquals(tableEventDatacenters, datacenters);
    }

    private void assertTableEventsAbsent(TableBackingStore backingStore, Set<String> datacenters, String table,
                                         String uuid) throws Exception {
        AstyanaxTableDAO tableDAO = newTableDAO(DC_US, backingStore);
        tableDAO.getTableEvents(table, uuid, tableDAO.getTableEventDatacenters())
                .filter(entry -> datacenters.contains(entry.getKey()))
                .forEach(entry -> fail());
    }

    private void assertReadyTableEventAbsent(TableBackingStore backingStore, String datacenter) throws Exception {
        assertNull(newTableDAO(datacenter, backingStore).getNextReadyTableEvent(datacenter));
    }


    private TableOptions newOptions(String placement) {
        return new TableOptionsBuilder().setPlacement(placement).build();
    }

    private FacadeOptions newFacadeOptions(String placement) {
        return new FacadeOptionsBuilder().setPlacement(placement).build();
    }

    private Audit newAudit() {
        return new AuditBuilder().build();
    }

    private void patchTableJsonTimestamp(TableBackingStore backingStore, String table, String dotPath, Instant when) {
        Delta delta = Deltas.literal(formatTimestamp(when));
        for (String key : Lists.reverse(Arrays.asList(dotPath.split("\\.")))) {
            delta = Deltas.mapBuilder().update(key, delta).build();
        }
        backingStore.update("__system:table", table, TimeUUIDs.newUUID(), delta, newAudit(), WriteConsistency.GLOBAL);
    }

    private void patchTableEventJsonTimestamp(TableBackingStore backingStore, String table, String datacenter, Set<String> registrants, Instant when) {
        UUID uuid = TimeUUIDs.uuidForTimeMillis(when.toEpochMilli());
        Delta delta = Deltas.mapBuilder().update("eventTime", Deltas.literal(uuid.toString())).build();
        delta = Deltas.mapBuilder().update(table, delta).build();
        delta = Deltas.mapBuilder().update("tasks", delta).build();
        MapDeltaBuilder mapDeltaBuilder = Deltas.mapBuilder();
        for (String registrant : registrants) {
            mapDeltaBuilder.update(registrant, delta);
        }

        Delta finalDelta = Deltas.mapBuilder().update("registrants", mapDeltaBuilder.build()).build();

        backingStore.update("__system:table_event_registry", datacenter, TimeUUIDs.newUUID(), finalDelta, newAudit(), WriteConsistency.GLOBAL);
    }

    private void registerTableEventListeners(TableBackingStore backingStore) throws Exception {
        Instant expirationTime = Instant.now().plus(30, ChronoUnit.DAYS);
        newTableDAO(DC_US, backingStore).registerTableListener(DC_US, expirationTime);
        newTableDAO(DC_EU, backingStore).registerTableListener(DC_EU, expirationTime);
        newTableDAO(DC_SA, backingStore).registerTableListener(DC_SA, expirationTime);
        newTableDAO(DC_ZZ, backingStore).registerTableListener(DC_ZZ, expirationTime);

    }

    private AstyanaxTableDAO newTableDAO(String dataCenter)
            throws Exception {
        return newTableDAO(dataCenter, (Clock) null);
    }

    private AstyanaxTableDAO newTableDAO(String dataCenter, Clock clock)
            throws Exception {
        return newTableDAO(newBackingStore(new MetricRegistry()), dataCenter, clock, new Date(0));
    }

    private AstyanaxTableDAO newTableDAO(TableBackingStore backingStore, String dataCenter, Clock clock, Date date)
            throws Exception {
        return newTableDAO(backingStore, dataCenter,
                mock(DataCopyDAO.class), mock(DataPurgeDAO.class), date, clock);
    }

    private AstyanaxTableDAO newTableDAO(String datacenter, TableBackingStore backingStore)
            throws Exception{
        return newTableDAO(backingStore, datacenter, mock(DataCopyDAO.class), mock(DataPurgeDAO.class), new Date(0), null);
    }

    private AstyanaxTableDAO newTableDAO(TableBackingStore backingStore, String dataCenter,
                                         DataCopyDAO dataCopyDAO, DataPurgeDAO dataPurgeDAO,
                                         final Date fullConsistencyTimestamp)
            throws Exception {
        return newTableDAO(backingStore, dataCenter, dataCopyDAO, dataPurgeDAO, fullConsistencyTimestamp, null);
    }

    private AstyanaxTableDAO newTableDAO(TableBackingStore backingStore, String dataCenter,
                                         DataCopyDAO dataCopyDAO, DataPurgeDAO dataPurgeDAO,
                                         final Date fullConsistencyTimestamp, final Clock clock)
            throws Exception {
        PlacementFactory placementFactory = newPlacementFactory(dataCenter);

        FullConsistencyTimeProvider fullConsistencyTimeProvider = mock(FullConsistencyTimeProvider.class);
        when(fullConsistencyTimeProvider.getMaxTimeStamp(Mockito.<String>any())).then(new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) {
                return fullConsistencyTimestamp.getTime();
            }
        });

        CacheRegistry cacheRegistry = mock(CacheRegistry.class);
        when(cacheRegistry.lookup("tables", true)).thenReturn(mock(CacheHandle.class));

        ValueStore<Boolean> tableChangesEnabled = mock(ValueStore.class);
        when(tableChangesEnabled.get()).thenReturn(true);

        AstyanaxTableDAO tableDAO = new AstyanaxTableDAO(mock(LifeCycleRegistry.class),
                "__system", PL_GLOBAL, 16, ImmutableMap.<String, Long>of(),
                placementFactory, new PlacementCache(placementFactory),
                dataCenter, mock(RateLimiterCache.class), dataCopyDAO, dataPurgeDAO,
                fullConsistencyTimeProvider, tableChangesEnabled, cacheRegistry,
                ImmutableMap.of(PL_ZZ_MOVING, PL_ZZ), new ObjectMapper(), clock);
        tableDAO.setBackingStore(backingStore);
        return tableDAO;
    }

    private InMemoryDataStore newBackingStore(MetricRegistry metricRegistry) {
        InMemoryDataStore store = new InMemoryDataStore(metricRegistry);
        store.createTable("__system:table", newOptions(PL_GLOBAL), ImmutableMap.<String, Object>of(), newAudit());
        store.createTable("__system:table_uuid", newOptions(PL_GLOBAL), ImmutableMap.<String, Object>of(), newAudit());
        store.createTable("__system:table_unpublished_databus_events", newOptions(PL_GLOBAL), ImmutableMap.<String, Object>of(), newAudit());
        store.createTable("__system:table_event_registry", newOptions(PL_GLOBAL), ImmutableMap.of(), newAudit());
        return store;
    }

    private PlacementFactory newPlacementFactory(String dataCenter)
            throws Exception {
        PlacementFactory placementFactory = mock(PlacementFactory.class);

        DataCenter dc1 = newDataCenter(DC_US, true);
        DataCenter dc2 = newDataCenter(DC_EU, false);
        DataCenter dc3 = newDataCenter(DC_SA, false);
        DataCenter dc4 = newDataCenter(DC_ZZ, false);

        when(placementFactory.getDataCenters(PL_GLOBAL)).thenReturn(ImmutableList.of(dc1, dc2, dc3, dc4));
        when(placementFactory.getDataCenters(PL_US_EU)).thenReturn(ImmutableList.of(dc1, dc2));
        when(placementFactory.getDataCenters(PL_EU_APAC)).thenReturn(ImmutableList.of(dc2, dc3));
        when(placementFactory.getDataCenters(PL_US)).thenReturn(ImmutableList.of(dc1));
        when(placementFactory.getDataCenters(PL_EU)).thenReturn(ImmutableList.of(dc2));
        when(placementFactory.getDataCenters(PL_APAC)).thenReturn(ImmutableList.of(dc3));
        when(placementFactory.getDataCenters(PL_ZZ)).thenReturn(ImmutableList.of(dc4));
        when(placementFactory.getDataCenters(PL_ZZ_MOVING)).thenReturn(ImmutableList.of(dc4));

        for (String placement : new String[] {PL_GLOBAL, PL_US_EU, PL_EU_APAC, PL_US, PL_EU, PL_APAC, PL_ZZ, PL_ZZ_MOVING}) {
            when(placementFactory.isValidPlacement(placement)).thenReturn(true);
            for (DataCenter memberDataCenter : placementFactory.getDataCenters(placement)) {
                if (dataCenter.equals(memberDataCenter.getName())) {
                    when(placementFactory.isAvailablePlacement(placement)).thenReturn(true);
                }
            }

            CassandraKeyspace keyspace = mock(CassandraKeyspace.class);
            when(keyspace.getClusterName()).thenReturn("emo_cluster");

            Placement placementObject = mock(Placement.class);
            when(placementObject.getName()).thenReturn(placement);
            when(placementFactory.newPlacement(placement)).thenReturn(placementObject);
            when(placementObject.getKeyspace()).thenReturn(keyspace);
        }

        return placementFactory;
    }

    private DataCenter newDataCenter(String name, boolean system) {
        return new DefaultDataCenter(name, URI.create("/"), URI.create("/"), system, name, ImmutableList.<String>of());
    }

    private Storage getStorage(TableJson table, String uuid) {
        for (Storage storage : table.getStorages()) {
            if (uuid.equals(storage.getUuidString())) {
                return storage;
            }
        }
        return null;
    }

    private String formatTimestamp(Instant timestamp) {
        return JsonMap.TimestampAttribute.format(checkNotNull(timestamp));
    }
}
