package com.bazaarvoice.emodb.databus;

import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.databus.db.generic.CachingSubscriptionDAO;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

public class DatabusConfiguration {

    /**
     * Which Cassandra keyspace should the Databus use to store events?
     */
    @Valid
    @NotNull
    @JsonProperty("cassandra")
    private CassandraConfiguration _cassandraConfiguration;

    @Valid
    @NotNull
    @JsonProperty("longPollKeepAliveThreadCount")
    private Optional<Integer> _longPollKeepAliveThreadCount = Optional.absent();

    @Valid
    @NotNull
    @JsonProperty("longPollPollingThreadCount")
    private Optional<Integer> _longPollPollingThreadCount = Optional.absent();

    /**
     * The following is only necessary during the period while the legacy subscription cache is upgraded to the current
     * implementation.
     */
    @Valid
    @NotNull
    @JsonProperty("subscriptionCacheInvalidation")
    private CachingSubscriptionDAO.CachingMode _subscriptionCacheInvaliation = CachingSubscriptionDAO.CachingMode.normal;

    public CassandraConfiguration getCassandraConfiguration() {
        return _cassandraConfiguration;
    }

    public DatabusConfiguration setCassandraConfiguration(CassandraConfiguration cassandraConfiguration) {
        _cassandraConfiguration = cassandraConfiguration;
        return this;
    }

    public Optional<Integer> getLongPollKeepAliveThreadCount() {
        return _longPollKeepAliveThreadCount;
    }

    public DatabusConfiguration setLongPollKeepAliveThreadCount(Integer longPollKeepAliveThreadCount) {
        _longPollKeepAliveThreadCount = Optional.of(longPollKeepAliveThreadCount);
        return this;
    }

    public Optional<Integer> getLongPollPollingThreadCount() {
        return _longPollPollingThreadCount;
    }

    public DatabusConfiguration setLongPollPollingThreadCount(Integer longPollPollingThreadCount) {
        _longPollPollingThreadCount = Optional.of(longPollPollingThreadCount);
        return this;
    }

    public CachingSubscriptionDAO.CachingMode getSubscriptionCacheInvaliation() {
        return _subscriptionCacheInvaliation;
    }

    public DatabusConfiguration setSubscriptionCacheInvaliation(CachingSubscriptionDAO.CachingMode subscriptionCacheInvaliation) {
        _subscriptionCacheInvaliation = subscriptionCacheInvaliation;
        return this;
    }
}
