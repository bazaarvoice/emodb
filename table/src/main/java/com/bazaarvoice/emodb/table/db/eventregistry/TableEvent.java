package com.bazaarvoice.emodb.table.db.eventregistry;

import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.uuid.TimeUUIDs;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class TableEvent {

    private static final String ACTION = "action";
    private static final String STORAGE = "storage";
    private static final String READY = "ready";
    private static final String EVENT_TIME = "eventTime";

    public enum Action {
        DROP,
        PROMOTE
    }

    private final Action _action;
    private final String _storage;
    private final boolean _ready;
    private final UUID _eventTime;

    public TableEvent(Action action, String storage) {
        this(action, storage, false, TimeUUIDs.newUUID());
    }

    @JsonCreator
    public TableEvent(@JsonProperty(ACTION) Action action,
                      @JsonProperty(STORAGE) String storage,
                      @JsonProperty(READY) Boolean ready,
                      @JsonProperty(EVENT_TIME) UUID eventTime) {
        _action = requireNonNull(action);
        _storage = requireNonNull(storage);
        _ready = requireNonNull(ready);
        _eventTime = requireNonNull(eventTime);
    }

    public Action getAction() {
        return _action;
    }

    public String getStorage() {
        return _storage;
    }

    public boolean isReady() {
        return _ready;
    }

    public UUID getEventTime() {
        return _eventTime;
    }

    public Map<String, Object> newFullEventMap() {
        return ImmutableMap.of(
                ACTION, _action.toString(),
                STORAGE, _storage,
                READY, _ready,
                EVENT_TIME, _eventTime.toString()
        );
    }

    public Delta newReadyDelta() {
        return Deltas.conditional(
                Conditions.mapBuilder().matches(STORAGE, Conditions.equal(_storage)).build(),
                Deltas.mapBuilder().put(READY, true).build());
    }

    public Delta newCompleteDelta() {
        return Deltas.conditional(
                Conditions.mapBuilder().matches(STORAGE, Conditions.equal(_storage)).build(),
                Deltas.delete()
        );
    }
}
