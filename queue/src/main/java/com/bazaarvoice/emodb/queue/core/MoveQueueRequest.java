package com.bazaarvoice.emodb.queue.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class MoveQueueRequest {

    private final String _from;
    private final String _to;

    @JsonCreator
    public MoveQueueRequest(@JsonProperty ("from") String from, @JsonProperty ("to") String to) {
        _from = requireNonNull(from, "from");
        _to = requireNonNull(to, "to");
    }

    public String getFrom() {
        return _from;
    }

    public String getTo() {
        return _to;
    }
}
