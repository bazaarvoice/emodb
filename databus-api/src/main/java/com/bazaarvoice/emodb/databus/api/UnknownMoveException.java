package com.bazaarvoice.emodb.databus.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Raised when a query is made on a move that does not exist.
 */
@JsonIgnoreProperties ({"cause", "localizedMessage", "stackTrace"})
public class UnknownMoveException extends RuntimeException {
    private final String _id;

    @JsonCreator
    public UnknownMoveException(@JsonProperty ("id") String id) {
        super("Unknown move: " + id);
        _id = id;
    }

    public String getId() {
        return _id;
    }
}