package com.bazaarvoice.emodb.table.db.eventregistry;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Deltas;
import com.bazaarvoice.emodb.sor.delta.MapDeltaBuilder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TableEventRegistrant {

    private static final String EXPIRATION_TIME = "expirationTime";
    private static final String TASKS = "tasks";

    private final Instant _expirationTime;
    private final Map<String, TableEvent> _tasks;

    @JsonCreator
    public TableEventRegistrant(@JsonProperty(EXPIRATION_TIME) Long expirationTime,
                                @Nullable @JsonProperty(TASKS) Map<String, TableEvent> tasks) {
        _expirationTime = Instant.ofEpochMilli(requireNonNull(expirationTime));
        _tasks = Optional.ofNullable(tasks).orElse(Collections.emptyMap());
    }

    public Instant getExpirationTime() {
        return _expirationTime;
    }

    public Map<String, TableEvent> getTasks() {
        return _tasks;
    }

    public TableEvent getTableEventForTableAndStorage(String table, String uuid) {
        return Optional.ofNullable(_tasks.get(table))
                .filter(tableEvent -> tableEvent.getStorage().equals(uuid))
                .orElse(null);

    }

    public Delta markTaskAsReadyIfExists(String table, String uuid, TableEvent.Action action) {
        return Optional.ofNullable(_tasks.get(table))
                .filter(tableEvent -> tableEvent.getStorage().equals(uuid) && tableEvent.getAction().equals(action))
                .map(tableEvent -> Deltas.mapBuilder().update(TASKS,
                        Deltas.mapBuilder().update(table, tableEvent.newReadyDelta()).build()).build())
                .orElse(null);
    }

    public Delta markTaskAsCompleteIfExists(String table, String uuid) {
        return Optional.ofNullable(_tasks.get(table))
                .filter(tableEvent -> tableEvent.getStorage().equals(uuid))
                .map(tableEvent -> Deltas.mapBuilder().update(TASKS,
                        Deltas.mapBuilder().update(table, tableEvent.newCompleteDelta()).build()).build())
                .orElse(null);
    }

    public Delta newTask(String table, TableEvent task, Instant now) {
        MapDeltaBuilder mapDeltaBuilder = Deltas.mapBuilder();
        mapDeltaBuilder.put(table, task.newFullEventMap());
        Condition isNotExpired = Conditions.mapBuilder().matches(EXPIRATION_TIME, Conditions.ge(now.toEpochMilli())).build();
        return Deltas.conditional(isNotExpired, Deltas.mapBuilder().update(TASKS, mapDeltaBuilder.build()).build(), Deltas.delete());
    }

    public static Delta newRegistrant(Instant now, Instant expirationTime) {

        Condition isExpired = Conditions.mapBuilder().matches(EXPIRATION_TIME, Conditions.le(now.toEpochMilli())).build();

        return Deltas.mapBuilder()
                .put(EXPIRATION_TIME, expirationTime.toEpochMilli())
                .update(TASKS, Deltas.conditional(isExpired, Deltas.literal(ImmutableMap.of())))
                .build();
    }
}
