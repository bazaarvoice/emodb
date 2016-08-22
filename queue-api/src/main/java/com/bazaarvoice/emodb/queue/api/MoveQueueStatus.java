package com.bazaarvoice.emodb.queue.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MoveQueueStatus {
    private final String _from;
    private final String _to;
    private final Status _status;

    public enum Status {
        IN_PROGRESS,
        COMPLETE,
        ERROR
    }

    @JsonCreator
    public MoveQueueStatus(@JsonProperty ("from") String from,
                           @JsonProperty ("to") String to,
                           @JsonProperty ("status") Status status) {
        _from = from;
        _to = to;
        _status = status;
    }

    public String getFrom() {
        return _from;
    }

    public String getTo() {
        return _to;
    }

    public Status getStatus() {
        return _status;
    }
}
