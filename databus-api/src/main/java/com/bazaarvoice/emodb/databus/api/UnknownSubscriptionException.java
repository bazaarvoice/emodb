package com.bazaarvoice.emodb.databus.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace"})
public class UnknownSubscriptionException extends RuntimeException {
    private final String _subscription;

    public UnknownSubscriptionException() {
        _subscription = null;
    }

    public UnknownSubscriptionException(String subscription) {
        super(subscription);
        _subscription = subscription;
    }

    @JsonCreator
    public UnknownSubscriptionException(@JsonProperty("message") String message, @JsonProperty("subscription") String subscription) {
        super(message);
        _subscription = subscription;
    }

    public UnknownSubscriptionException(String message, Throwable cause) {
        super(message, cause);
        _subscription = null;
    }

    public String getSubscription() {
        return _subscription;
    }
}
