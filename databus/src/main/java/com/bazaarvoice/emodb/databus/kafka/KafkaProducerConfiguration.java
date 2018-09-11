package com.bazaarvoice.emodb.databus.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Properties;

/**
 * Configuration properties for the eventProducer
 */
public class KafkaProducerConfiguration {

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_ACKS = "all";
    private static final int DEFAULT_RETRIES = 0;
    private static final int DEFAULT_BATCH_SIZE = 16384;
    private static final int DEFAULT_LINGER_MS = 1;
    private static final int DEFAULT_BUFFER_MEMORY = 33554432;
    private static final String DEFAULT_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String DEFAULT_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.ByteBufferSerializer";
    private static final String DEFAULT_CLIENT_ID = "emodb-kafka-producer";

    @Valid
    @NotNull
    @JsonProperty("bootstrap.servers")
    private String _bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;

    @Valid
    @NotNull
    @JsonProperty("acks")
    private String _acks = DEFAULT_ACKS;

    @Valid
    @NotNull
    @JsonProperty("retries")
    private int _retries = DEFAULT_RETRIES;

    @Valid
    @NotNull
    @JsonProperty("batch.size")
    private int _batchSize = DEFAULT_BATCH_SIZE;

    @Valid
    @NotNull
    @JsonProperty("linger.ms")
    private int _lingerMs = DEFAULT_LINGER_MS;

    @Valid
    @NotNull
    @JsonProperty("buffer.memory")
    private int _bufferMemory = DEFAULT_BUFFER_MEMORY;

    @Valid
    @NotNull
    @JsonProperty("key.serializer")
    private String _keySerializer = DEFAULT_KEY_SERIALIZER;

    @Valid
    @NotNull
    @JsonProperty("value.serializer")
    private String _valueSerializer = DEFAULT_VALUE_SERIALIZER;

    @Valid
    @NotNull
    @JsonProperty("client.id")
    private String _clientId = DEFAULT_CLIENT_ID;


    public String getBootstrapServers() { return _bootstrapServers; }
    public void setBootstrapServers( String bootstrapServers) { _bootstrapServers = bootstrapServers; }

    public String getAcks() { return _acks; }
    public void setAcks( String acks) { _acks = acks; }

    public int getRetriess() { return _retries; }
    public void setRetriess( int retries) { _retries = retries; }

    public int getBatchSize() { return _batchSize; }
    public void setBatchSize( int batchSize) { _batchSize = batchSize; }

    public int getLingerMs() { return _lingerMs; }
    public void setLingerMs( int lingerMs) { _lingerMs = lingerMs; }

    public int getBufferMemory() { return _bufferMemory; }
    public void setBufferMemory( int bufferMemory) { _bufferMemory = bufferMemory; }

    public String getKeySerializer() { return _keySerializer; }
    public void setKeySerializer( String keySerializer) { _keySerializer = keySerializer; }

    public String getValueSerializer() { return _valueSerializer; }
    public void setValueSerializer( String valueSerializer) { _valueSerializer = valueSerializer; }

    public String getClientId() { return _clientId; }
    public void setClientId( String clientId) { _clientId = clientId; }

    public Properties getProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", _bootstrapServers);
        props.put("acks", _acks);
        props.put("retries", _retries);
        props.put("batch.size", _batchSize);
        props.put("linger.ms", _lingerMs);
        props.put("buffer.memory", _bufferMemory);
        props.put("key.serializer", _keySerializer);
        props.put("value.serializer", _valueSerializer);
        props.put("client.id", _clientId);
        return props;
    }

}
