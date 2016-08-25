package com.bazaarvoice.emodb.databus.api;

import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Thrown when an unauthorized databus operation is performed on a subscription, such when a subscription created with
 * one API key is polled using a different key.
 */
@JsonIgnoreProperties({"cause", "localizedMessage", "stackTrace", "message", "suppressed"})
public class UnauthorizedSubscriptionException extends UnauthorizedException {
    private final String _subscription;

    public UnauthorizedSubscriptionException() {
        _subscription = null;
    }

    public UnauthorizedSubscriptionException(String subscription) {
        super(subscription);
        _subscription = subscription;
    }

    @JsonCreator
    public UnauthorizedSubscriptionException(@JsonProperty("reason") String message, @JsonProperty("subscription") String subscription) {
        super(message);
        _subscription = subscription;
    }

    public String getSubscription() {
        return _subscription;
    }
}