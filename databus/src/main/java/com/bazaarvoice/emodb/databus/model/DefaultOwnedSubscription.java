package com.bazaarvoice.emodb.databus.model;

import com.bazaarvoice.emodb.databus.api.DefaultSubscription;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.fasterxml.jackson.annotation.JsonValue;
import org.joda.time.Duration;

import java.util.Date;

/**
 * Default implementation of {@link OwnedSubscription}.  The JSON serialized version of this class does not include
 * any ownership information so it is safe to return to client-facing interfaces where a {@link Subscription}
 * is expected.
 */
public class DefaultOwnedSubscription implements OwnedSubscription {
    private final Subscription _subscription;
    private final String _ownerID;

    public DefaultOwnedSubscription(String name, Condition tableFilter,
                                    Date expiresAt, Duration eventTtl,
                                    String ownerID) {
        _subscription = new DefaultSubscription(name, tableFilter, expiresAt, eventTtl);
        _ownerID = ownerID;
    }

    @Override
    public String getOwnerId() {
        return _ownerID;
    }

    @Override
    public String getName() {
        return _subscription.getName();
    }

    @Override
    public Condition getTableFilter() {
        return _subscription.getTableFilter();
    }

    @Override
    public Date getExpiresAt() {
        return _subscription.getExpiresAt();
    }

    @Override
    public Duration getEventTtl() {
        return _subscription.getEventTtl();
    }

    /**
     * The JSON representation should not include any ownership attributes.
     */
    @JsonValue
    public Subscription getSubscription() {
        return _subscription;
    }
}
