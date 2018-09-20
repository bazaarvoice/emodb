package com.bazaarvoice.emodb.databus;

import com.bazaarvoice.emodb.common.cassandra.CassandraConfiguration;
import com.bazaarvoice.emodb.databus.db.generic.CachingSubscriptionDAO;
import com.bazaarvoice.emodb.databus.kafka.KafkaConsumerConfiguration;
import com.bazaarvoice.emodb.databus.kafka.KafkaProducerConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.apache.commons.lang.mutable.MutableBoolean;

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
    private CachingSubscriptionDAO.CachingMode _subscriptionCacheInvalidation = CachingSubscriptionDAO.CachingMode.normal;

    @Valid
    @NotNull
    @JsonProperty("masterFanoutPartitions")
    private int _masterFanoutPartitions = 4;

    @Valid
    @NotNull
    @JsonProperty("dataCenterFanoutPartitions")
    private int _dataCenterFanoutPartitions = 4;

    @Valid
    @NotNull
    @JsonProperty("kafka.enabled")
    private Boolean _kafkaEnabled = new Boolean(true);

    @Valid
    @NotNull
    @JsonProperty("kafka.test.resolver.forceRetry")
    private Boolean _kafkaTestForceRetry = new Boolean(false);

    @Valid
    @NotNull
    @JsonProperty("kafka.test.resolver.retry.forceRetryToFail")
    private Boolean _kafkaTestForceRetryToFail = new Boolean(false);

    @Valid
    @NotNull
    @JsonProperty("kafka.eventProducerConfiguration")
    private KafkaProducerConfiguration _eventProducerConfiguration = new KafkaProducerConfiguration();

    @Valid
    @NotNull
    @JsonProperty("kafka.resolvedEventProducerConfiguration")
    private KafkaProducerConfiguration _resolvedEventProducerConfiguration = new KafkaProducerConfiguration();

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

    public Boolean getKafkaEnabled() { return _kafkaEnabled; }

    public void setKafkaEnabled(boolean enabled) { _kafkaEnabled = new Boolean(enabled); }

    public Boolean getKafkaTestForceRetry() { return _kafkaTestForceRetry; }

    public void getKafkaTestForceRetry(boolean forceRetry) { _kafkaTestForceRetry = new Boolean(forceRetry); }

    public Boolean getKafkaTestForceRetryToFail() { return _kafkaTestForceRetryToFail; }

    public void setKafkaTestForceRetryToFail(boolean forceRetryToFail) { _kafkaTestForceRetryToFail = new Boolean(forceRetryToFail); }


    public KafkaProducerConfiguration getEventProducerConfiguration() { return _eventProducerConfiguration; }

    public void setEventProducerConfiguration(KafkaProducerConfiguration eventProducerConfiguration) {
        _eventProducerConfiguration = eventProducerConfiguration;
    }

    public KafkaProducerConfiguration getResolvedEventProducerConfiguration() { return _resolvedEventProducerConfiguration; }

    public void setResolvedEventProducerConfiguration(KafkaProducerConfiguration resolvedEventProducerConfiguration) {
        _resolvedEventProducerConfiguration = resolvedEventProducerConfiguration;
    }

}
