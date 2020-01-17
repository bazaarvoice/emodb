package com.bazaarvoice.emodb.table.db.eventregistry;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.MapDeltaBuilder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@JsonIgnoreProperties (ignoreUnknown = true)
public class TableEventDatacenter {

    private static final String REGISTRANTS = "registrants";
    private static final String DATACENTER = Intrinsic.ID;

    private final Map<String, TableEventRegistrant> _registrants;
    private final String _dataCenter;

    @JsonCreator
    public TableEventDatacenter(@Nullable @JsonProperty(REGISTRANTS) Map<String, TableEventRegistrant> registrants,
                                @JsonProperty(DATACENTER) String dataCenter) {

        _registrants = Optional.ofNullable(registrants).orElse(Collections.emptyMap());
        _dataCenter = requireNonNull(dataCenter);
    }

    public Map<String, TableEventRegistrant> getRegistrants() {
        return _registrants;
    }

    public String getDataCenter() {
        return _dataCenter;
    }

    public Delta markTableEventAsReady(String table, String uuid) {

        List<Map.Entry<String, Delta>> deltas = new ArrayList<>();

        _registrants.forEach((registrationId, tableEventRegistrant) -> {
            Optional.ofNullable(tableEventRegistrant.markTaskAsReadyIfExists(table, uuid))
                    .map(delta -> new AbstractMap.SimpleEntry<>(registrationId, delta))
                    .ifPresent(deltas::add);
        });

        if (deltas.isEmpty()) {
            return null;
        }

        MapDeltaBuilder mapDeltaBuilder = Deltas.mapBuilder();

        deltas.forEach(delta -> mapDeltaBuilder.update(delta.getKey(), delta.getValue()));

        return Deltas.mapBuilder().update(REGISTRANTS, mapDeltaBuilder.build()).build();
    }

    public List<TableEvent> getTableEventsForTableAndStorage(String table, String storage) {
        return _registrants.values().stream()
                .map(tableEventRegistrant -> tableEventRegistrant.getTableEventForTableAndStorage(table, storage))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    public Delta markTableEventAsComplete(String registrationId, String table, String uuid) {
        MapDeltaBuilder mapDeltaBuilder = Deltas.mapBuilder();

        Optional.ofNullable(_registrants.get(registrationId))
                .map(registrant -> mapDeltaBuilder.update(REGISTRANTS, Deltas.mapBuilder().update(registrationId, registrant.markTaskAsCompleteIfExists(table, uuid)).build()));

        return mapDeltaBuilder.build();
    }

    public Delta newTableEvent(String table, TableEvent tableEvent, Instant now) {
        MapDeltaBuilder mapDeltaBuilder = Deltas.mapBuilder();

        _registrants.forEach((registationId, registrant) -> {
            mapDeltaBuilder.update(registationId, registrant.newTask(table, tableEvent, now));
        });

        return Deltas.mapBuilder().update(REGISTRANTS, mapDeltaBuilder.build()).build();
    }
}
