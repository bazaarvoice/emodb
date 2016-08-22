package com.bazaarvoice.emodb.table.db;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Exception raised in the specific case where a table is looked up by an attribute other than it's name
 * (typically its UUID) and the table exists but is not associated with any active tables.
 *
 * If the lookup was formerly associated with a named table then {@link #getPriorTable()} will return the
 * prior tables name.  This can happen if the table was deleted and recreated and the active lookup is by
 * an attribute of the deleted version of the table.
 */
@JsonIgnoreProperties ({"cause", "localizedMessage", "stackTrace"})
public class DroppedTableException extends RuntimeException {
    private final String _priorTable;

    public DroppedTableException() {
        _priorTable = null;
    }

    public DroppedTableException(String priorTable) {
        super(priorTable);
        _priorTable = priorTable;
    }

    @JsonCreator
    public DroppedTableException(@JsonProperty("message") String message, @JsonProperty("priorTable") String priorTable) {
        super(message);
        _priorTable = priorTable;
    }

    public String getPriorTable() {
        return _priorTable;
    }
}
