package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class UnknownTableException extends RuntimeException {
    private final String _table;

    public UnknownTableException() {
        _table = null;
    }

    public UnknownTableException(String table) {
        super(table);
        _table = table;
    }

    @JsonCreator
    public UnknownTableException(@JsonProperty ("message") String message, @JsonProperty("table") String table) {
        super(message);
        _table = table;
    }

    public UnknownTableException(String message, Throwable cause) {
        super(message, cause);
        _table = null;
    }

    public String getTable() {
        return _table;
    }
}
