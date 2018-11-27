package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

/**
 * All changes for a point-in-time, including a content delta, delta history, audit information, compaction information.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({"timestamp", "id", "delta", "compaction", "history"})
public final class Change {

    private final UUID _id;
    @Nullable
    private final Delta _delta;
    @Nullable
    private final Compaction _compaction;
    @Nullable
    private final History _history;
    private final Set<String> _tags;

    Change(@JsonProperty("id") UUID id,
           @JsonProperty("delta") @Nullable Delta delta,
           @JsonProperty("compaction") @Nullable Compaction compaction,
           @JsonProperty("history") @Nullable History history,
           @JsonProperty("tags") @Nullable Set<String> tags) {
        _id = requireNonNull(id, "changeId");
        _delta = delta;
        _compaction = compaction;
        _history = history;
        _tags = Optional.ofNullable(tags).orElse(Collections.emptySet());
    }

    // Add a human-readable timestamp for debugging.  This gets serialized into the JSON
    public Date getTimestamp() {
        return TimeUUIDs.getDate(_id);
    }

    public UUID getId() {
        return _id;
    }

    @Nullable
    public Delta getDelta() {
        return _delta;
    }

    @Nullable
    public Compaction getCompaction() {
        return _compaction;
    }

    @Nullable
    public History getHistory() {
        return _history;
    }

    public Set<String> getTags() {
        return _tags;
    }
}
