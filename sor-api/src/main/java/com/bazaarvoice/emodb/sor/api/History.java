package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.sor.delta.Delta;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.Map;
import java.util.UUID;

/**
 * Captures point in time of a document life-cycle.
 * This represents a historical delta with the following properties:
 * - changeId
 * - raw delta
 * - resolved content up till the point of this delta. Think of it as a snapshot in time.
 */
@JsonInclude (JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties (ignoreUnknown = true)
@JsonPropertyOrder ({"changeId", "delta", "content"})
public class History {
    private UUID _changeId;
    private Map<String, Object> _content;
    private Delta _delta;

    public History(@JsonProperty ("changeId") UUID changeId,
                   @JsonProperty ("content") Map<String, Object> content,
                   @JsonProperty ("delta") Delta delta) {
        _changeId = changeId;
        _content = content;
        _delta = delta;
    }

    public UUID getChangeId() {
        return _changeId;
    }

    public Map<String, Object> getContent() {
        return _content;
    }

    public Delta getDelta() {
        return _delta;
    }
}
