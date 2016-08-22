package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * Exception that is thrown when attempting to access data in a locally inaccessible placement.  For example, if a
 * table exists in an EU-only placement then this exception would be thrown when reading its contents from a
 * US EmoDB cluster.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class UnknownPlacementException extends IllegalArgumentException {
    private final String _placement;
    // If this exception was thrown in the context of reading a table then this attribute will be set to the table name.
    private String _table;

    public UnknownPlacementException() {
        _placement = null;
    }

    public UnknownPlacementException(String placement) {
        super(placement);
        _placement = placement;
    }

    public UnknownPlacementException(String message, String placement) {
        this(message, placement, null);
    }

    @JsonCreator
    public UnknownPlacementException(@JsonProperty("message") String message, @JsonProperty("placement") String placement,
                                     @Nullable @JsonProperty("table") String table) {
        super(message);
        _placement = placement;
        _table = table;
    }

    public UnknownPlacementException(String message, Throwable cause) {
        super(message, cause);
        _placement = null;
    }

    public String getPlacement() {
        return _placement;
    }

    public String getTable() {
        return _table;
    }

    public void setTable(String table) {
        _table = table;
    }
}
