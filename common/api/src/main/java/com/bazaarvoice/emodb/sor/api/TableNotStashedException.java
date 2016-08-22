package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Exception thrown when Stash is available but the specific table requested has no stash presence.  The root cause
 * for this is typically that the table is empty, but surface the exception so the client can validate.
 */
@JsonIgnoreProperties ({"cause", "localizedMessage", "stackTrace"})
public class TableNotStashedException extends RuntimeException {

    private final String _table;

    public TableNotStashedException(String table) {
        this("Table not stashed: " + table, table);
    }

    @JsonCreator
    public TableNotStashedException(@JsonProperty ("message") String message,
                                    @JsonProperty ("table") String table) {
        super(message);
        _table = table;
    }

    public String getTable() {
        return _table;
    }
}
