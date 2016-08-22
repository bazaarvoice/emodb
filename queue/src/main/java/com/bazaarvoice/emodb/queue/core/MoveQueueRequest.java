package com.bazaarvoice.emodb.queue.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkNotNull;

public class MoveQueueRequest {

    private final String _from;
    private final String _to;

    @JsonCreator
    public MoveQueueRequest(@JsonProperty ("from") String from, @JsonProperty ("to") String to) {
        _from = checkNotNull(from, "from");
        _to = checkNotNull(to, "to");
    }

    public String getFrom() {
        return _from;
    }

    public String getTo() {
        return _to;
    }
}
