package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.sor.api.FacadeExistsException;
import com.bazaarvoice.emodb.sor.api.FacadeOptions;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableBackingStore;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class AstyanaxTableDAOTest {
    private AstyanaxTableDAO _sysDcTableDAO;

    private static final String _systemDcName = "datacenter1";
    private static final String _dc2Name = "datacenter2";
    private static final String _dc3Name = "datacenter3";
    private static final String _dc4Name = "datacenter4";
    private static final String _ugcPlacement = "ugc_global:ugc";
    private static final String _catPlacement = "catalog_global:cat";
    private static final String _remotePlacement = "app_remote:remote";
    private static final String _subsetPlacement = "susbset_remote:subset";
    private static final String _tableMetaData = "{\n" +
            "  \"uuid\": \"46fda11f48e40cf0\",\n" +
            "  \"attributes\": {\n" +
            "    \"type\": \"review\",\n" +
            "    \"client\": \"TestCustomer\"\n" +
            "  },\n" +
            "  \"storage\": {\n" +
            "    \"46fda11f48e40cf0\": {\n" +
            "      \"placement\": \"ugc_global:ugc\"\n" +
            "    },\n" +
            "    \"ac775a6e5de9b56\": {\n" +
            "      \"facade\": true,\n" +
            "      \"placement\": \"catalog_global:cat\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"~id\": \"review:testcustomer\",\n" +
            "  \"~table\": \"__system_sor:table\",\n" +
            "  \"~version\": 2,\n" +
            "  \"~signature\": \"27acf749dceb65ae5a20408c49bf76a0\",\n" +
            "  \"~deleted\": false,\n" +
            "  \"~firstUpdateAt\": \"2014-01-26T03:18:03.200Z\",\n" +
            "  \"~lastUpdateAt\": \"2014-01-26T03:18:14.416Z\"\n" +
            "}";

    @BeforeMethod
    public void setupMock() throws IOException {
        _sysDcTableDAO = newTableDAO(_systemDcName);
    }

    private PlacementFactory newPlacementFactory(String dataCenter) {
        PlacementFactory placementFactory = mock(PlacementFactory.class);

        DataCenter systemDataCenter = mock(DataCenter.class);
        when(systemDataCenter.getName()).thenReturn(_systemDcName);

        DataCenter dc2 = mock(DataCenter.class);
        when(dc2.getName()).thenReturn(_dc2Name);

        DataCenter dc3 = mock(DataCenter.class);
        when(dc3.getName()).thenReturn(_dc3Name);

        DataCenter dc4 = mock(DataCenter.class);
        when(dc4.getName()).thenReturn(_dc4Name);

        when(placementFactory.getDataCenters(_ugcPlacement)).thenReturn(ImmutableList.of(systemDataCenter));
        when(placementFactory.getDataCenters(_catPlacement)).thenReturn(ImmutableList.of(systemDataCenter, dc2, dc3));
        when(placementFactory.getDataCenters(_subsetPlacement)).thenReturn(ImmutableList.of(dc2, dc3));
        when(placementFactory.getDataCenters(_remotePlacement)).thenReturn(ImmutableList.of(dc4));

        for (String placement : new String[]{_ugcPlacement, _catPlacement, _subsetPlacement, _remotePlacement}) {
            for (DataCenter memberDataCenter : placementFactory.getDataCenters(placement)) {
                if (dataCenter.equals(memberDataCenter.getName())) {
                    when(placementFactory.isAvailablePlacement(placement)).thenReturn(true);
                }
            }
        }

        return placementFactory;
    }

    private AstyanaxTableDAO newTableDAO(String dataCenter) throws IOException {
        String systemTableNamespace = "systemNamespace";
        String systemTablePlacement = "systemTablePlacement";
        String systemTable = systemTableNamespace + ":table";
        String systemTableMetadataChanges = systemTableNamespace + ":table_unpublished_databus_events";
        String systemTableEventRegistry = systemTableNamespace + ":table_event_registry";
        String systemTableUuid = systemTableNamespace + ":table_uuid";
        String systemDataCenterTable = systemTableNamespace + ":data_center";
        AstyanaxTableDAO tableDAO = new AstyanaxTableDAO(mock(LifeCycleRegistry.class),
                systemTableNamespace, systemTablePlacement, 16,
                ImmutableMap.of(systemTable, 123L, systemTableUuid, 345L, systemDataCenterTable, 567L, systemTableMetadataChanges, 980L, systemTableEventRegistry, 246L),
                newPlacementFactory(dataCenter), mock(PlacementCache.class), dataCenter,
                mock(RateLimiterCache.class), mock(DataCopyDAO.class), mock(DataPurgeDAO.class),
                mock(FullConsistencyTimeProvider.class), mock(ValueStore.class), mock(CacheRegistry.class),
                ImmutableMap.of(), new ObjectMapper(), mock(Clock.class));

        TableBackingStore tableBackingStore = mock(TableBackingStore.class);
        Map<String, Object> tableMap = JsonHelper.fromJson(_tableMetaData, new TypeReference<Map<String, Object>>() {});
        when(tableBackingStore.get(anyString(), anyString(), any(ReadConsistency.class)))
                .thenReturn(tableMap);

        tableDAO.setBackingStore(tableBackingStore);

        return tableDAO;
    }

    @Test
    public void testTableFromJson()
            throws Exception {
        /**
         * Essentially, this is responsible for returning the right table relative to the data center we are in.
         * If the "master" table placement is available in our data center, then that's what we want.
         * If the "master" table placement is not visible in our data center, then look for it's facade.
         *      1. If a facade is found, then return the facade
         *      2. If no facade is found, then return the master table anyway
         */

        TableJson hm = new TableJson(JsonHelper.fromJson(_tableMetaData, new TypeReference<Map<String, Object>>() {}));

        AstyanaxTableDAO dc2TableDao = newTableDAO(_dc2Name);
        Table table = dc2TableDao.tableFromJson(hm);

        assertTrue(table.isFacade(), "The facade should be returned");


        Set<String> dataCenters = ImmutableList.copyOf(table.getDataCenters()).stream().map(DataCenter::getName).collect(Collectors.toSet());
        assertEquals(dataCenters, Sets.newHashSet(_dc2Name, _dc3Name), "dc2, and dc3 should be the only data centers returned for this facade");

        table = _sysDcTableDAO.tableFromJson(hm);

        assertFalse(table.isFacade(), "The actual table should be returned");
    }

    @Test
    public void facadeNotAllowedIfTableExists()
            throws Exception {
        // Should not allow a facade if master placement is available in the datacenter
        FacadeOptions options = new FacadeOptions(_catPlacement);
        assertFalse(_sysDcTableDAO.checkFacadeAllowed("review:testcustomer", options),
                "Facade should not be created");
    }

    @Test(expectedExceptions = FacadeExistsException.class)
    public void facadeNotAllowedInOverlappingDataCenter()
            throws Exception {
        // Should not allow a facade in dc3 since a facade is created in catalog_global keyspace
        // that overlaps with subset placement
        FacadeOptions options = new FacadeOptions(_subsetPlacement);
        _sysDcTableDAO.checkFacadeAllowed("review:testcustomer", options);
    }

    @Test
    public void facadeAllowedInDataCenter4()
            throws Exception {
        // Should allow a facade in data center 4 as there is no overlapping placement here
        // The only placement in data cneter 4 is app_remote.
        FacadeOptions options = new FacadeOptions(_remotePlacement);
        assertTrue(_sysDcTableDAO.checkFacadeAllowed("review:testcustomer", options),
                "Facade should be allowed to be created in this data center");
    }
}
