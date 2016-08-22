package com.bazaarvoice.emodb.common.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;

@JsonIgnoreProperties ({"cause", "localizedMessage", "stackTrace"})
public class JsonStreamProcessingException extends RuntimeException {

    public JsonStreamProcessingException(Throwable cause) {
        //noinspection ThrowableResultOfMethodCallIgnored
        this(Throwables.getRootCause(cause).getMessage());
    }

    @JsonCreator
    public JsonStreamProcessingException(@JsonProperty ("message") String message) {
        super(message);
    }
}
