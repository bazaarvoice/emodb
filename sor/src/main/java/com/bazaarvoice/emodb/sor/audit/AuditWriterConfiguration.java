package com.bazaarvoice.emodb.sor.audit;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.util.Size;
import org.joda.time.Duration;

import javax.annotation.Nonnull;

public class AuditWriterConfiguration {

    @JsonProperty("logBucket")
    @Nonnull
    private String _logBucket;

    @JsonProperty("logBucketRegion")
    @Nonnull
    private String _logBucketRegion = "us-east-1";

    @JsonProperty("logPath")
    @Nonnull
    private String _logPath;

    /* Only required if not using default credentials provider */
    @JsonProperty("s3AccessKey")
    private String _s3AccessKey;

    /* Only required if not using default credentials provider */
    @JsonProperty("s3SecretKey")
    private String _s3SecretKey;

    @JsonProperty("maxFileSize")
    private long _maxFileSize = Size.megabytes(50).toBytes();

    @JsonProperty("maxBatchTime")
    private Duration _maxBatchTime = Duration.standardMinutes(2);

    @JsonProperty("stagingDir")
    private String _stagingDir;

    @JsonProperty("logFilePrefix")
    private String _logFilePrefix = "audit-log";

    @JsonProperty("s3Endpoint")
    private String _s3Endpoint;

    /* Useful for local testing, impractical to disable in production */
    @JsonProperty("fileTransfersEnabled")
    private boolean _fileTransfersEnabled = true;

    @Nonnull
    public String getLogBucket() {
        return _logBucket;
    }

    public AuditWriterConfiguration setLogBucket(@Nonnull String logBucket) {
        _logBucket = logBucket;
        return this;
    }

    @Nonnull
    public String getLogBucketRegion() {
        return _logBucketRegion;
    }

    public AuditWriterConfiguration setLogBucketRegion(@Nonnull String logBucketRegion) {
        _logBucketRegion = logBucketRegion;
        return this;
    }

    @Nonnull
    public String getLogPath() {
        return _logPath;
    }

    public AuditWriterConfiguration setLogPath(@Nonnull String logPath) {
        _logPath = logPath;
        return this;
    }

    public String getS3AccessKey() {
        return _s3AccessKey;
    }

    public AuditWriterConfiguration setS3AccessKey(String s3AccessKey) {
        _s3AccessKey = s3AccessKey;
        return this;
    }

    public String getS3SecretKey() {
        return _s3SecretKey;
    }

    public AuditWriterConfiguration setS3SecretKey(String s3SecretKey) {
        _s3SecretKey = s3SecretKey;
        return this;
    }

    public long getMaxFileSize() {
        return _maxFileSize;
    }

    public AuditWriterConfiguration setMaxFileSize(long maxFileSize) {
        _maxFileSize = maxFileSize;
        return this;
    }

    public Duration getMaxBatchTime() {
        return _maxBatchTime;
    }

    public AuditWriterConfiguration setMaxBatchTime(Duration maxBatchTime) {
        _maxBatchTime = maxBatchTime;
        return this;
    }

    public String getStagingDir() {
        return _stagingDir;
    }

    public AuditWriterConfiguration setStagingDir(String stagingDir) {
        _stagingDir = stagingDir;
        return this;
    }

    public String getLogFilePrefix() {
        return _logFilePrefix;
    }

    public AuditWriterConfiguration setLogFilePrefix(String logFilePrefix) {
        _logFilePrefix = logFilePrefix;
        return this;
    }

    public String getS3Endpoint() {
        return _s3Endpoint;
    }

    public void setS3Enpoint(String s3Endpoint) {
        _s3Endpoint = s3Endpoint;
    }

    public boolean isFileTransfersEnabled() {
        return _fileTransfersEnabled;
    }

    public AuditWriterConfiguration setFileTransfersEnabled(boolean fileTransfersEnabled) {
        _fileTransfersEnabled = fileTransfersEnabled;
        return this;
    }
}
