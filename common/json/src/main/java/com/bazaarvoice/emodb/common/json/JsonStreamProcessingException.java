package com.bazaarvoice.emodb.common.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class JsonStreamProcessingException extends RuntimeException {

    public JsonStreamProcessingException(Throwable cause) {
        this(getRootCauseMessage(cause));
    }

    private static String getRootCauseMessage(Throwable cause) {
        while (cause != null && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause != null ? cause.getMessage() : null;
    }

    @JsonCreator
    public JsonStreamProcessingException(@JsonProperty("message") String message) {
        super(message);
    }
}
