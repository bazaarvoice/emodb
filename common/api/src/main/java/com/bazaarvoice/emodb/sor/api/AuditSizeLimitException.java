package com.bazaarvoice.emodb.sor.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Thrown when a single audit record exceeds the maximum size permitted.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class AuditSizeLimitException extends RuntimeException {
    private final long _size;

    @JsonCreator
    public AuditSizeLimitException(@JsonProperty("message") String message, @JsonProperty ("size") long size) {
        super(message);
        _size = size;
    }

    public long getSize() {
        return _size;
    }
}