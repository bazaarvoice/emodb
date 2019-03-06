package com.bazaarvoice.emodb.databus.api;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

import java.time.Duration;
import java.util.Date;

public final class DefaultSubscription implements Subscription {
    private final String _name;
    private final Condition _tableFilter;
    private final Date _expiresAt;
    private final Duration _eventTtl;

    public DefaultSubscription(String name, Condition tableFilter, Date expiresAt, Duration eventTtl) {
        _name = name;
        _tableFilter = tableFilter;
        _expiresAt = expiresAt;
        _eventTtl = eventTtl;
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
            return Objects.equal(this.getName(), toCompare.getName()) &&
                    Objects.equal(this.getTableFilter(), toCompare.getTableFilter()) &&
                    Objects.equal(this.getExpiresAt(), toCompare.getExpiresAt()) &&
                    Objects.equal(this.getEventTtl(), toCompare.getEventTtl());
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_name, _tableFilter, _expiresAt, _eventTtl);
    }
}
