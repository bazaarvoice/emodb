package com.bazaarvoice.emodb.queue.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Raised when a query is made on a move that does not exist.
 */
@JsonIgnoreProperties ({"cause", "localizedMessage", "stackTrace"})
public class UnknownMoveException extends RuntimeException {
    private final String _id;

    public UnknownMoveException(String id) {
        this(id, "Unknown move: " + id);
    }

    @JsonCreator
    public UnknownMoveException(@JsonProperty ("id") String id, @JsonProperty ("message") String message) {
        super(message);
        _id = id;
    }

    public String getId() {
        return _id;
    }
}
