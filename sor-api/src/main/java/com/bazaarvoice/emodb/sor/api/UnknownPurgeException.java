package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Raised when a query is made on a purge that does not exist.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class UnknownPurgeException extends RuntimeException {
    private final String _id;

    @JsonCreator
    public UnknownPurgeException(@JsonProperty("id") String id) {
        super("Unknown purge: " + id);
        _id = id;
    }

    public String getId() {
        return _id;
    }
}