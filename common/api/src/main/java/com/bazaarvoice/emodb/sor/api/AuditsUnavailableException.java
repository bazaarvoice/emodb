package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Raised when a query is made on a purge that does not exist.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class AuditsUnavailableException extends RuntimeException {
    @JsonCreator
    public AuditsUnavailableException() {
        super("Audit data is no longer accessable via EmoDB app servers.");
    }
}