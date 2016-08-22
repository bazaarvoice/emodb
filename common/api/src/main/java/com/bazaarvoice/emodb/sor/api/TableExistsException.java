package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties ({"cause", "localizedMessage", "stackTrace"})
public class TableExistsException extends RuntimeException {
    private final String _table;

    public TableExistsException() {
        _table = null;
    }

    public TableExistsException(String table) {
        super(table);
        _table = table;
    }

    @JsonCreator
    public TableExistsException(@JsonProperty("message") String message, @JsonProperty("table") String table) {
        super(message);
        _table = table;
    }

    public String getTable() {
        return _table;
    }
}
