package com.bazaarvoice.emodb.databus.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

@JsonInclude (JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties (ignoreUnknown = true)
public class ReplaySubscriptionRequest {

    private String _ownerId;
    private String _subscription;
    @Nullable
    private Date _since;

    @JsonCreator
    public ReplaySubscriptionRequest(@JsonProperty ("ownerId") String ownerId,
                                     @JsonProperty ("subscription") String subscription,
                                     @JsonProperty ("since") @Nullable Date since) {
        _ownerId = checkNotNull(ownerId, "ownerId");
        _subscription = checkNotNull(subscription, "subscription");
        _since = since;
    }

    public String getOwnerId() {
        return _ownerId;
    }

    public String getSubscription() {
        return _subscription;
    }

    public Date getSince() {
        return _since;
    }
}
