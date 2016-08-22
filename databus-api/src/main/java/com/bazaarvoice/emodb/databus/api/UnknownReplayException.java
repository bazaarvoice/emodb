package com.bazaarvoice.emodb.databus.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Raised when a query is made on a reaply that does not exist.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class UnknownReplayException extends RuntimeException {
    private final String _id;

    @JsonCreator
    public UnknownReplayException(@JsonProperty("id") String id) {
        super("Unknown replay: " + id);
        _id = id;
    }

    public String getId() {
        return _id;
    }
}