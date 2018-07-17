package com.bazaarvoice.emodb.databus.api;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.time.Duration;
import java.util.Date;

@JsonDeserialize(as = DefaultSubscription.class)
public interface Subscription {

    String getName();

    Condition getTableFilter();

    Date getExpiresAt();

    Duration getEventTtl();
}
