package com.bazaarvoice.emodb.web.scanner.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Optional;

/**
 * Scanner-specific service configurations.
 */
public class ScannerConfiguration {

    private static final int DEFAULT_SCAN_THREAD_COUNT = 8;
    private static final int DEFAULT_UPLOAD_THREAD_COUNT = 12;
    private static final String DEFAULT_SCAN_STATUS_TABLE = "__system_scan_upload";
    private static final String DEFAULT_SCAN_REQUEST_TABLE = "__system_scan_request";
    private static final int DEFAULT_MAX_S3_CONNECTIONS = 60;

    // Controls whether to use SQS or EmoDB queues for the scan's queue implementation
    @Valid
    @NotNull
    @JsonProperty ("useSQSQueues")
    private boolean _useSQSQueues = true;

    // If using EmoDB queues, the API key to use
    @Valid
    @NotNull
    @JsonProperty("scannerApiKey")
    private Optional<String> _scannerApiKey = Optional.empty();

    // Maximum number of scan threads that can run concurrently on a single server.  Default is 8.
    @Valid
    @NotNull
    @JsonProperty ("scanThreadCount")
    private int _scanThreadCount = DEFAULT_SCAN_THREAD_COUNT;

    // Maximum number of upload threads that can run concurrently on a single server.  Default is 12.
    @Valid
    @NotNull
    @JsonProperty ("uploadThreadCount")
    private int _uploadThreadCount = DEFAULT_UPLOAD_THREAD_COUNT;

    // Name of the table which holds scan status entries.
    @Valid
    @NotNull
    @JsonProperty ("scanStatusTable")
    private String _scanStatusTable = DEFAULT_SCAN_STATUS_TABLE;

    // Name of the table which holds scan request entries.
    @Valid
    @NotNull
    @JsonProperty ("scanRequestTable")
    private String _scanRequestTable = DEFAULT_SCAN_REQUEST_TABLE;

    // Maximum number of open S3 connections
    @Valid
    @NotNull
    @JsonProperty ("maxS3Connections")
    private int _maxS3Connections = DEFAULT_MAX_S3_CONNECTIONS;

    // Optional URI for the S3 proxy host
    @Valid
    @NotNull
    @JsonProperty("s3Proxy")
    private Optional<String> _s3Proxy = Optional.empty();

    // If using S3, optionally assume the provided role
    @Valid
    @NotNull
    @JsonProperty("s3AssumeRole")
    private Optional<String> _s3AssumeRole = Optional.empty();
    @Valid
    @NotNull
    @JsonProperty("notifications")
    private ScannerNotificationConfig _notifications = new ScannerNotificationConfig();

    @Valid
    @NotNull
    @JsonProperty ("scheduledScans")
    private Map<String, ScheduledScanConfiguration> _scheduledScans = Maps.newHashMap();

    @Valid
    @NotNull
    @JsonProperty("pendingScanRangeQueueName")
    private Optional<String> _pendingScanRangeQueueName = Optional.empty();

    @Valid
    @NotNull
    @JsonProperty("completeScanRangeQueueName")
    private Optional<String> _completeScanRangeQueueName = Optional.empty();

    public boolean isUseSQSQueues() {
        return _useSQSQueues;
    }

    public ScannerConfiguration setUseSQSQueues(boolean useSQSQueues) {
        _useSQSQueues = useSQSQueues;
        return this;
    }

    public int getScanThreadCount() {
        return _scanThreadCount;
    }

    public Optional<String> getScannerApiKey() {
        return _scannerApiKey;
    }

    public void setScannerApiKey(Optional<String> scannerApiKey) {
        _scannerApiKey = scannerApiKey;
    }

    public ScannerConfiguration setScanThreadCount(int scanThreadCount) {
        _scanThreadCount = scanThreadCount;
        return this;
    }

    public int getUploadThreadCount() {
        return _uploadThreadCount;
    }

    public ScannerConfiguration setUploadThreadCount(int uploadThreadCount) {
        _uploadThreadCount = uploadThreadCount;
        return this;
    }

    public String getScanStatusTable() {
        return _scanStatusTable;
    }

    public ScannerConfiguration setScanStatusTable(String scanStatusTable) {
        _scanStatusTable = scanStatusTable;
        return this;
    }

    public String getScanRequestTable() {
        return _scanRequestTable;
    }

    public ScannerConfiguration setScanRequestTable(String scanRequestTable) {
        _scanRequestTable = scanRequestTable;
        return this;
    }

    public int getMaxS3Connections() {
        return _maxS3Connections;
    }

    public void setMaxS3Connections(int maxS3Connections) {
        _maxS3Connections = maxS3Connections;
    }

    public Optional<String> getS3Proxy() {
        return _s3Proxy;
    }

    public ScannerConfiguration setS3Proxy(Optional<String> s3Proxy) {
        _s3Proxy = s3Proxy;
        return this;
    }

    public ScannerNotificationConfig getNotifications() {
        return _notifications;
    }

    public void setNotifications(ScannerNotificationConfig notifications) {
        _notifications = notifications;
    }

    public Map<String, ScheduledScanConfiguration> getScheduledScans() {
        return _scheduledScans;
    }

    public ScannerConfiguration setScheduledScans(Map<String, ScheduledScanConfiguration> scheduledScans) {
        _scheduledScans = scheduledScans;
        return this;
    }

    public Optional<String> getPendingScanRangeQueueName() {
        return _pendingScanRangeQueueName;
    }

    public ScannerConfiguration setPendingScanRangeQueueName(Optional<String> pendingScanRangeQueueName) {
        _pendingScanRangeQueueName = pendingScanRangeQueueName;
        return this;
    }

    public Optional<String> getCompleteScanRangeQueueName() {
        return _completeScanRangeQueueName;
    }

    public ScannerConfiguration setCompleteScanRangeQueueName(Optional<String> completeScanRangeQueueName) {
        _completeScanRangeQueueName = completeScanRangeQueueName;
        return this;
    }

    public Optional<String> getS3AssumeRole() {
        return _s3AssumeRole;
    }

    public ScannerConfiguration setS3AssumeRole(Optional<String> s3AssumeRole) {
        _s3AssumeRole = s3AssumeRole;
        return this;
    }
}
