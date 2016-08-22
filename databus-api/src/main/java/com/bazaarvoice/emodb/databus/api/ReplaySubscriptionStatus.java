package com.bazaarvoice.emodb.databus.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ReplaySubscriptionStatus {
    private final String _subscription;
    private final Status _status;

    public enum Status {
        IN_PROGRESS,
        COMPLETE,
        ERROR
    }

    @JsonCreator
    public ReplaySubscriptionStatus(@JsonProperty ("subscription") String subscription,
                                    @JsonProperty ("status") Status status) {
        _subscription = subscription;
        _status = status;
    }

    public String getSubscription() {
        return _subscription;
    }

    public Status getStatus() {
        return _status;
    }
}
