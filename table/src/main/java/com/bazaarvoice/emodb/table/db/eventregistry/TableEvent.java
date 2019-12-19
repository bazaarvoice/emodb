package com.bazaarvoice.emodb.table.db.eventregistry;

import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TableEvent {

    private static final String EVENT_KEY = "eventKey";
    private static final String ACTION = "action";
    private static final String UUID = "uuid";
    private static final String READY = "ready";

    public enum Action {
        DROP,
        PROMOTE
    }

    private final Action _action;
    private final String _uuid;
    private final boolean _ready;

    public TableEvent(Action action, String uuid) {
        this(action, uuid, false);
    }

    @JsonCreator
    public TableEvent(@JsonProperty(ACTION) Action action,
                      @JsonProperty(UUID) String uuid,
                      @JsonProperty(READY) Boolean ready) {
        _action = requireNonNull(action);
        _uuid = requireNonNull(uuid);
        _ready = requireNonNull(ready);
    }

    public Action getAction() {
        return _action;
    }

    public String getUuid() {
        return _uuid;
    }

    public boolean isReady() {
        return _ready;
    }

    public Map<String, Object> newFullEventMap() {
        return ImmutableMap.of(
                ACTION, _action.toString(),
                UUID, _uuid,
                READY, _ready
        );
    }

    public Delta newReadyDelta() {
        return Deltas.conditional(
                Conditions.mapBuilder().matches(UUID, Conditions.equal(_uuid)).build(),
                Deltas.mapBuilder().put(READY, true).build());
    }

    public Delta newCompleteDelta() {
        return Deltas.conditional(
                Conditions.mapBuilder().matches(UUID, Conditions.equal(_uuid)).build(),
                Deltas.delete()
        );
    }
}
