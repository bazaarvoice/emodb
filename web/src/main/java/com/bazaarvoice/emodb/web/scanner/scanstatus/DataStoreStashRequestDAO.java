package com.bazaarvoice.emodb.web.scanner.scanstatus;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.TableOptionsBuilder;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;

import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link StashRequestDAO} implementation which uses a DataStore table for persistence.
 */
public class DataStoreStashRequestDAO implements StashRequestDAO {

    private final DataStore _dataStore;
    private final String _tableName;
    private final String _tablePlacement;
    private volatile boolean _tableChecked = false;

    @Inject
    public DataStoreStashRequestDAO(DataStore dataStore,
                                    @StashRequestTable String tableName,
                                    @StashRequestTablePlacement String tablePlacement) {
        _dataStore = dataStore;
        _tableName = tableName;
        _tablePlacement = tablePlacement;
    }

    /**
     * Returns the scan request table name.  On the first call it also verifies that the table exists, then skips this
     * check on future calls.
     */
    private String getTable() {
        if (!_tableChecked) {
            if (!_dataStore.getTableExists(_tableName)) {
                _dataStore.createTable(
                        _tableName,
                        new TableOptionsBuilder().setPlacement(_tablePlacement).build(),
                        ImmutableMap.<String, Object>of(),
                        new AuditBuilder().setLocalHost().setComment("Create scan request table").build());
            }

            _tableChecked = true;
        }

        return _tableName;
    }

    @Override
    public void requestStash(String scanId, StashRequest request) {
        checkNotNull(scanId, "scanId");
        checkNotNull(request, "request");

        _dataStore.update(getTable(), scanId, TimeUUIDs.newUUID(),
                Deltas.mapBuilder()
                        .update("requests",
                                Deltas.mapBuilder()
                                        .update(request.getRequestedBy(), Deltas.literal(JsonHelper.convert(request.getRequestTime(), String.class)))
                                        .build())
                        .build(),
                new AuditBuilder().setComment("Added scan request").set("requestedBy", request.getRequestedBy()).build());
    }

    @Override
    public void undoRequestStash(String scanId, StashRequest request) {
        checkNotNull(scanId, "scanId");
        checkNotNull(request, "request");

        _dataStore.update(getTable(), scanId, TimeUUIDs.newUUID(),
                Deltas.conditional(Conditions.isDefined(),
                    Deltas.mapBuilder()
                            .update("requests",
                                    Deltas.conditional(Conditions.isDefined(),
                                            Deltas.mapBuilder()
                                                    .update(request.getRequestedBy(), Deltas.delete())
                                                    .build()))
                            .build()),
                new AuditBuilder().setComment("Removed scan request").set("requestedBy", request.getRequestedBy()).build());
    }

    @Override
    public Set<StashRequest> getRequestsForStash(String scanId) {
        Map<String, Object> map = _dataStore.get(getTable(), scanId);
        if (Intrinsic.isDeleted(map)) {
            return ImmutableSet.of();
        }
        Map<String, Date> requestMap = JsonHelper.convert(
                map.getOrDefault("requests", ImmutableMap.of()), new TypeReference<Map<String, Date>>(){});

        return requestMap.entrySet().stream()
                .map(entry -> new StashRequest(entry.getKey(), entry.getValue()))
                .collect(Collectors.toSet());
    }
}
