package com.bazaarvoice.emodb.common.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * Exception thrown when the service is unavailable or overloaded and the caller should retry again later.
 * Corresponds to an HTTP 503 response.
 */
@JsonIgnoreProperties ({"cause", "localizedMessage", "stackTrace"})
public class ServiceUnavailableException extends RuntimeException {

    private Integer _retryAfterSeconds;

    public ServiceUnavailableException() {
        super();
    }

    @JsonCreator
    public ServiceUnavailableException(@JsonProperty("message") String message,
                                       @JsonProperty("retryAfterSeconds") @Nullable Integer retryAfterSeconds) {
        super(message);
        _retryAfterSeconds = retryAfterSeconds;
    }

    public Integer getRetryAfterSeconds() {
        return _retryAfterSeconds;
    }
}
