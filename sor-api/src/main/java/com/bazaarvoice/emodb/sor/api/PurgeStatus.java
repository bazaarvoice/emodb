package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PurgeStatus {
    private final String _table;
    private final Status _status;

    public enum Status {
        IN_PROGRESS,
        COMPLETE,
        ERROR
    }

    @JsonCreator
    public PurgeStatus(@JsonProperty ("table") String table,
                       @JsonProperty ("status") Status status) {
        _table = table;
        _status = status;
    }

    public String getTable() {
        return _table;
    }

    public Status getStatus() {
        return _status;
    }
}
