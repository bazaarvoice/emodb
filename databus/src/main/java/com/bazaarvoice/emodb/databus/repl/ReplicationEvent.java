package com.bazaarvoice.emodb.databus.repl;

import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;
import java.util.Set;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public class ReplicationEvent {
    private final String _id;
    private final String _table;
    private final String _key;
    private final UUID _changeId;
    private final Set<String> _tags;

    public ReplicationEvent(String id, UpdateRef ref) {
        this(id, ref.getTable(), ref.getKey(), ref.getChangeId(), ref.getTags());
    }

    @JsonCreator
    public ReplicationEvent(@JsonProperty("id") String id, @JsonProperty("table") String table,
                            @JsonProperty("key") String key, @JsonProperty("changeId") UUID changeId,
                            @Nullable @JsonProperty("tags") Set<String> tags) {
        _id = requireNonNull(id, "id");
        _table = requireNonNull(table, "table");
        _key = requireNonNull(key, "key");
        _changeId = requireNonNull(changeId, "changeId");
        _tags = (tags == null) ? ImmutableSet.<String>of() : tags;
    }

    public String getId() {
        return _id;
    }

    public String getTable() {
        return _table;
    }

    public String getKey() {
        return _key;
    }

    public UUID getChangeId() {
        return _changeId;
    }

    public Set<String> getTags() {
        return _tags;
    }

}
