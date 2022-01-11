package com.bazaarvoice.emodb.databus;

import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.databus.db.generic.CachingSubscriptionDAO;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Optional;

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
    private Optional<Integer> _longPollKeepAliveThreadCount = Optional.empty();

    @Valid
    @NotNull
    @JsonProperty("longPollPollingThreadCount")
    private Optional<Integer> _longPollPollingThreadCount = Optional.empty();

    /**
     * The following is only necessary during the period while the legacy subscription cache is upgraded to the current
     * implementation.
     */
    @Valid
    @NotNull
    @JsonProperty("subscriptionCacheInvalidation")
    private CachingSubscriptionDAO.CachingMode _subscriptionCacheInvalidation = CachingSubscriptionDAO.CachingMode.normal;

    @Valid
    @NotNull
    @JsonProperty("masterFanoutPartitions")
    private int _masterFanoutPartitions = 4;

    @Valid
    @NotNull
    @JsonProperty("dataCenterFanoutPartitions")
    private int _dataCenterFanoutPartitions = 4;

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

    public CachingSubscriptionDAO.CachingMode getSubscriptionCacheInvalidation() {
        return _subscriptionCacheInvalidation;
    }

    public DatabusConfiguration setSubscriptionCacheInvalidation(CachingSubscriptionDAO.CachingMode subscriptionCacheInvalidation) {
        _subscriptionCacheInvalidation = subscriptionCacheInvalidation;
        return this;
    }

    public int getMasterFanoutPartitions() {
        return _masterFanoutPartitions;
    }

    public DatabusConfiguration setMasterFanoutPartitions(int masterFanoutPartitions) {
        _masterFanoutPartitions = masterFanoutPartitions;
        return this;
    }

    public int getDataCenterFanoutPartitions() {
        return _dataCenterFanoutPartitions;
    }

    public DatabusConfiguration setDataCenterFanoutPartitions(int dataCenterFanoutPartitions) {
        _dataCenterFanoutPartitions = dataCenterFanoutPartitions;
        return this;
    }
}
