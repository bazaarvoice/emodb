package com.bazaarvoice.emodb.databus.api;

import com.bazaarvoice.emodb.common.json.serde.TimestampDurationSerializer;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.joda.time.Duration;
import com.google.common.base.Objects;
import java.util.Date;

public final class DefaultSubscription implements Subscription {
    private final String _name;
    private final Condition _tableFilter;
    @JsonProperty("eventTtl")
    @JsonSerialize(using = TimestampDurationSerializer.class)
    private final Date _expiresAt;

    private final Duration _eventTtl;

    public DefaultSubscription(@JsonProperty("name") String name,
                               @JsonProperty("tableFilter") Condition tableFilter,
                               @JsonProperty("expiresAt") Date expiresAt,
                               @JsonProperty("eventTtl") Duration eventTtl) {
        _name = name;
        _tableFilter = tableFilter;
        _expiresAt = expiresAt;
        _eventTtl = eventTtl;
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

    @Override
    public Duration getEventTtl() {
        return _eventTtl;
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
