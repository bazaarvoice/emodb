package com.bazaarvoice.emodb.common.api;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Exception thrown when the caller attempts to perform an unauthorized action.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace", "message", "suppressed"})
public class UnauthorizedException extends RuntimeException {

    public UnauthorizedException() {
        super("not authorized");
    }

    public UnauthorizedException(@JsonProperty("reason") String message) {
        super(message);
    }

    /**
     * For historical reasons the JSON returned for unauthorized exceptions uses a key called "reason", not "message".
     */
    public String getReason() {
        return getMessage();
    }
}
