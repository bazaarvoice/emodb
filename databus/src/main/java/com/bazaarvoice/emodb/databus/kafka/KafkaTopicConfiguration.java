package com.bazaarvoice.emodb.databus.kafka;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Properties;
import java.util.UUID;

/**
 * Configuration properties for a Kafka topic
 */
public class KafkaTopicConfiguration {

    private static final String DEFAULT_CLEANUP_POLICY = "delete";
    private static final String DEFAULT_COMPRESSION_TYPE = "uncompressed";
    private static final long DEFAULT_DELETE_RETENTION_MS = 86400000;
    private static final int DEFAULT_MAX_MESSAGE_BYTES = 1000012;
    private static final double DEFAULT_MIN_CLEANABLE_DIRTY_RATIO = 0.5;
    private static final int DEFAULT_MIN_INSYNC_REPLICAS = 1;
    private static final long DEFAULT_RETENTION_MS = 604800000;
    private static final int DEFAULT_PARTITIONS = 1;
    private static final int DEFAULT_REPLICATION_FACTOR = 1;
    private static final String DEFAULT_TOPIC_NAME = "default-topic-name";

    @Valid
    @NotNull
    @JsonProperty("cleanup.policy")
    private String _cleanupPolicy = DEFAULT_CLEANUP_POLICY;

    @Valid
    @NotNull
    @JsonProperty("compression.type")
    private String _compressionType = DEFAULT_COMPRESSION_TYPE;

    @Valid
    @NotNull
    @JsonProperty("delete.retention.ms")
    private long _deleteRetentionMs = DEFAULT_DELETE_RETENTION_MS;

    @Valid
    @NotNull
    @JsonProperty("max.message.bytes")
    private int _maxMessageBytes = DEFAULT_MAX_MESSAGE_BYTES;

    @Valid
    @NotNull
    @JsonProperty("min.cleanable.dirty.ratio")
    private double _minCleanableDirtyRatio = DEFAULT_MIN_CLEANABLE_DIRTY_RATIO;

    @Valid
    @NotNull
    @JsonProperty("min.insync.replicas")
    private int _minInsyncReplicas = DEFAULT_MIN_INSYNC_REPLICAS;

    @Valid
    @NotNull
    @JsonProperty("retention.ms")
    private long _retentionMs = DEFAULT_RETENTION_MS;

    @Valid
    @NotNull
    @JsonProperty("partitions")
    private int _partitions = DEFAULT_PARTITIONS;

    @Valid
    @NotNull
    @JsonProperty("replication.factor")
    private int _replicationFactor = DEFAULT_REPLICATION_FACTOR;

    @Valid
    @NotNull
    @JsonProperty("name")
    private String _topicName = DEFAULT_TOPIC_NAME;

    // Default constructor gets all default values
    public KafkaTopicConfiguration() {}

    // Used for testing when a config file is not provided
    public KafkaTopicConfiguration(String topicName) {
        _cleanupPolicy = DEFAULT_CLEANUP_POLICY;
        _compressionType = DEFAULT_COMPRESSION_TYPE;
        _deleteRetentionMs = DEFAULT_DELETE_RETENTION_MS;
        _maxMessageBytes = DEFAULT_MAX_MESSAGE_BYTES;
        _minCleanableDirtyRatio = DEFAULT_MIN_CLEANABLE_DIRTY_RATIO;
        _minInsyncReplicas = DEFAULT_MIN_INSYNC_REPLICAS;
        _retentionMs = DEFAULT_RETENTION_MS;
        _partitions = DEFAULT_PARTITIONS;
        _replicationFactor = DEFAULT_REPLICATION_FACTOR;
        _topicName = topicName;
    }

    public String getCleanupPolicy() { return _cleanupPolicy; }
    public void setCleanupPolicy( String cleanupPolicy) { _cleanupPolicy = cleanupPolicy; }

    public String getCompressionType() { return _compressionType; }
    public void setCompressionType( String compressionType) { _compressionType = compressionType; }

    public long getDeleteRetentionMs() { return _deleteRetentionMs; }
    public void setDeleteRetentionMs( long deleteRetentionMs) { _deleteRetentionMs = deleteRetentionMs; }

    public int getMaxMessageBytes() { return _maxMessageBytes; }
    public void setMaxMessageBytes( int maxMessageBytes) { _maxMessageBytes = maxMessageBytes; }

    public double getMinCleanableDirtyRatio() { return _minCleanableDirtyRatio; }
    public void setMinCleanableDirtyRatio( double minCleanableDirtyRatio) { _minCleanableDirtyRatio = minCleanableDirtyRatio; }

    public int getMinInsyncReplicas() { return _minInsyncReplicas; }
    public void setMinInsyncReplicas( int minInsyncReplicas) { _minInsyncReplicas = minInsyncReplicas; }

    public long getRetentionMs() { return _retentionMs; }
    public void setRetentionMs( long retentionMs) { _retentionMs = retentionMs; }

    public int getPartitions() { return _partitions; }
    public void setPartitions( int partitions) { _partitions = partitions; }

    public int getReplicationFactor() { return _replicationFactor; }
    public void setReplicationFactor( int replicationFactor) { _replicationFactor = replicationFactor; }

    public String getTopicName() { return _topicName; }
    public void setTopicName(String topicName) { _topicName = topicName; }


    public Properties getProps() {
        Properties props = new Properties();
        props.put("cleanup.policy", _cleanupPolicy);
        props.put("compression.type", _compressionType);
        props.put("delete.retention.ms", _deleteRetentionMs);
        props.put("max.message.bytes", _maxMessageBytes);
        props.put("min.cleanable.dirty.ratio", _minCleanableDirtyRatio);
        props.put("min.insync.replicas", _minInsyncReplicas);
        props.put("retention.ms", _retentionMs);
        props.put("partitions", _partitions);
        props.put("replication.factor", _replicationFactor);
        props.put("name", _topicName);
        return props;
    }

    public Properties getKafkaProps() {
        Properties props = new Properties();
        props.put("cleanup.policy", _cleanupPolicy);
        props.put("compression.type", _compressionType);
        props.put("delete.retention.ms", Long.toString(_deleteRetentionMs));
        props.put("max.message.bytes", Integer.toString(_maxMessageBytes));
        props.put("min.cleanable.dirty.ratio", Double.toString(_minCleanableDirtyRatio));
        props.put("min.insync.replicas", Integer.toString(_minInsyncReplicas));
        props.put("retention.ms", Long.toString(_retentionMs));
        return props;
    }

    public static Properties makeKafkaProps(String kafkaTopicCleanupPolicy, String kafkaTopicCompressionType, long kafkaTopicDeleteRetentionMs, int kafkaTopicMaxMessageBytes,
        double kafkaTopicMinCleanableDirtyRatio, int kafkaTopicMinInSyncReplicas, long kafkaTopicRetentionMs) {
        Properties props = new Properties();
        props.put("cleanup.policy", kafkaTopicCleanupPolicy);
        props.put("compression.type", kafkaTopicCompressionType);
        props.put("delete.retention.ms", Long.toString(kafkaTopicDeleteRetentionMs));
        props.put("max.message.bytes", Integer.toString(kafkaTopicMaxMessageBytes));
        props.put("min.cleanable.dirty.ratio", Double.toString(kafkaTopicMinCleanableDirtyRatio));
        props.put("min.insync.replicas", Integer.toString(kafkaTopicMinInSyncReplicas));
        props.put("retention.ms", Long.toString(kafkaTopicRetentionMs));
        return props;
    }


}
