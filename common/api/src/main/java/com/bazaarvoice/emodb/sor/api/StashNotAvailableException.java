package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Thrown when the cluster does not support Stash.
 */
@JsonIgnoreProperties ({"cause", "localizedMessage", "stackTrace"})
public class StashNotAvailableException extends RuntimeException {

    public StashNotAvailableException() {
    }

    @JsonCreator
    public StashNotAvailableException(@JsonProperty ("message") String message) {
        super(message);
    }
}
