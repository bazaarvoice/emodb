package com.bazaarvoice.emodb.databus.api;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.util.Date;
import java.util.Objects;

import static com.bazaarvoice.emodb.databus.api.Names.isLegalSubscriptionName;

public final class DefaultSubscription implements Subscription {
    // Currently, by default, event ttl limit is set to 365 days,
    // but that could be changed in future
    private static final Duration EVENT_TTL_LIMIT = Duration.ofDays(365);

    // Currently, by default, subscription ttl limit is set to 365 days,
    // but that could be changed in future
    private static final Duration SUBSCRIPTION_TTL_LIMIT = Duration.ofDays(365);

    private final String _name;
    private final Condition _tableFilter;
    private final Date _expiresAt;
    private final Duration _eventTtl;

    public DefaultSubscription(String name, Condition tableFilter, Date expiresAt, Duration eventTtl) {
        _name = validateName(name);
        _tableFilter = Objects.requireNonNull(tableFilter);
        _expiresAt =  Objects.requireNonNull(expiresAt);
        _eventTtl = validateEventTtl(eventTtl);
    }

    private static String validateName(final String name) {
        Objects.requireNonNull(name);

        if (!isLegalSubscriptionName(name)) {
            throw new IllegalArgumentException("Subscription name must be a lowercase ASCII string between 1 and 255 characters in length. " +
                    "Allowed punctuation characters are -.:@_ and the subscription name may not start with a single underscore character. " +
                    "An example of a valid subscription name would be 'polloi:review'.");
        }

        return name;
    }

    public static Duration validateEventTtl(final Duration eventTtl) {
        return validateTtl(eventTtl, Duration.ZERO, EVENT_TTL_LIMIT, "Event");
    }

    public static Duration validateSubscriptionTtl(final Duration eventTtl) {
        return validateTtl(eventTtl, Duration.ZERO, SUBSCRIPTION_TTL_LIMIT, "Subscription");
    }

    private static Duration validateTtl(final Duration eventTtl, final Duration lowerBound, final Duration upperBound, String type) {
        Objects.requireNonNull(eventTtl);

        if (eventTtl.compareTo(lowerBound) < 0) {
            throw new IllegalArgumentException(String.format("%sTtl must be >0", type));
        }

        if (eventTtl.compareTo(upperBound) > 0) {
            throw new IllegalArgumentException(String.format("%sTtl duration should be within %s days", type, upperBound.toDays()));
        }
        return eventTtl;
    }

    @JsonCreator
    private DefaultSubscription(@JsonProperty("name") String name,
                                @JsonProperty("tableFilter") Condition tableFilter,
                                @JsonProperty("expiresAt") Date expiresAt,
                                @JsonProperty("eventTtl") long eventTtlMillis) {
        this(name, tableFilter, expiresAt, Duration.ofMillis(eventTtlMillis));
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public Condition getTableFilter() {
        return _tableFilter;
    }

    @Override
    public Date getExpiresAt() {
        return _expiresAt;
    }

    @JsonIgnore
    @Override
    public Duration getEventTtl() {
        return _eventTtl;
    }

    @JsonProperty("eventTtl")
    private long getEventTtlMillis() {
        return _eventTtl.toMillis();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof Subscription) {
            Subscription toCompare = (Subscription) o;
            return Objects.equals(this.getName(), toCompare.getName()) &&
                    Objects.equals(this.getTableFilter(), toCompare.getTableFilter()) &&
                    Objects.equals(this.getExpiresAt(), toCompare.getExpiresAt()) &&
                    Objects.equals(this.getEventTtl(), toCompare.getEventTtl());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(_name, _tableFilter, _expiresAt, _eventTtl);
    }
}
