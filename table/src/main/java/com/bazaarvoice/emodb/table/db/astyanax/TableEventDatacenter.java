package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.MapDeltaBuilder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

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

    public Delta newTableEvent(String table, TableEvent tableEvent, Instant now) {
        MapDeltaBuilder mapDeltaBuilder = Deltas.mapBuilder();

        _registrants.forEach((registationId, registrant) -> {
            mapDeltaBuilder.update(registationId, registrant.newTask(table, tableEvent, now));
        });

        return Deltas.mapBuilder().update(REGISTRANTS, mapDeltaBuilder.build()).build();
    }
}
