package com.bazaarvoice.emodb.databus.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Properties;

/**
 * Configuration properties for the eventProducer
 */
public class KafkaConsumerConfiguration {

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_ACKS = "all";
    private static final int DEFAULT_RETRIES = 0;
    private static final int DEFAULT_BATCH_SIZE = 16384;
    private static final int DEFAULT_LINGER_MS = 1;
    private static final int DEFAULT_BUFFER_MEMORY = 33554432;
    private static final String DEFAULT_KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String DEFAULT_VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.ByteBufferDeserializer";
    private static final String DEFAULT_GROUP_ID = "emodb.comsumer";
    private static final int DEFAULT_MAX_POLL_INTERVAL_MS = 60000; // one minute max between polls?

    @Valid
    @NotNull
    @JsonProperty("bootstrap.servers")
    private String _bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;

    @Valid
    @NotNull
    @JsonProperty("group.id")
    private String _groupId = DEFAULT_GROUP_ID;

    @Valid
    @NotNull
    @JsonProperty("key.deserializer")
    private String _keyDeserializer = DEFAULT_KEY_DESERIALIZER;

    @Valid
    @NotNull
    @JsonProperty("value.deserializer")
    private String _valueDeserializer = DEFAULT_VALUE_DESERIALIZER;

    @Valid
    @NotNull
    @JsonProperty("max.poll.interval.ms")
    private int _maxPollIntervalMs = DEFAULT_MAX_POLL_INTERVAL_MS;

    public String getBootstrapServers() { return _bootstrapServers; }
    public void setBootstrapServers( String bootstrapServers) { _bootstrapServers = bootstrapServers; }

    public String getGroupId() { return _groupId; }
    public void setGroupId( String groupId) { _groupId = groupId; }

    public String getKeyDeserializer() { return _keyDeserializer; }
    public void setKeyDeserializer( String keyDeserializer) { _keyDeserializer = keyDeserializer; }

    public String getValueDeserializer() { return _valueDeserializer; }
    public void setValueDeserializer( String valueDeserializer) { _valueDeserializer = valueDeserializer; }

    public int getMaxPollIntervalMs() { return _maxPollIntervalMs; }
    public void setMaxPollIntervalMs(int maxPollIntervalMs) { _maxPollIntervalMs = maxPollIntervalMs; }

    public Properties getProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", _bootstrapServers);
        props.put("group.id", _groupId);
        props.put("key.deserializer", _keyDeserializer);
        props.put("value.deserializer", _valueDeserializer);
        props.put("max.poll.interval.ms", _maxPollIntervalMs);
        return props;
    }

}
