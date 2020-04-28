package com.bazaarvoice.emodb.blob.db.s3.config;

public class S3BucketConfiguration {
    private String name;
    private String region;
    private String roleArn;
    private String roleExternalId;
    private Boolean accelerateModeEnabled;
    private int scanBatchSize = 100;
    private int readRangeSize = 128 * 1024;         //128kb
    private int writeChunkSize = 5 * 1024 * 1024;   //5mb
    private S3ClientConfiguration s3ClientConfiguration;

    public S3BucketConfiguration(final String name,
                                 final String region,
                                 final String roleArn,
                                 final String roleExternalId,
                                 Boolean accelerateModeEnabled,
                                 int scanBatchSize,
                                 int readRangeSize,
                                 int writeChunkSize,
                                 S3ClientConfiguration s3ClientConfiguration) {
        this.name = name;
        this.region = region;
        this.roleArn = roleArn;
        this.roleExternalId = roleExternalId;
        this.accelerateModeEnabled = accelerateModeEnabled;
        this.scanBatchSize = scanBatchSize;
        this.readRangeSize = readRangeSize;
        this.writeChunkSize = writeChunkSize;
        this.s3ClientConfiguration = s3ClientConfiguration;
    }

    public S3BucketConfiguration() {
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public S3ClientConfiguration getS3ClientConfiguration() {
        return s3ClientConfiguration;
    }

    public void setS3ClientConfiguration(final S3ClientConfiguration s3ClientConfiguration) {
        this.s3ClientConfiguration = s3ClientConfiguration;
    }

    public String getName() {
        return name;
    }

    public String getRoleArn() {
        return roleArn;
    }

    public String getRoleExternalId() {
        return roleExternalId;
    }

    public void setName(final String name) {
        this.name = name;
    }

    public void setRoleArn(final String roleArn) {
        this.roleArn = roleArn;
    }

    public void setRoleExternalId(final String roleExternalId) {
        this.roleExternalId = roleExternalId;
    }

    public Boolean getAccelerateModeEnabled() {
        return accelerateModeEnabled;
    }

    public void setAccelerateModeEnabled(final Boolean accelerateModeEnabled) {
        this.accelerateModeEnabled = accelerateModeEnabled;
    }

    public int getScanBatchSize() {
        return scanBatchSize;
    }

    public void setScanBatchSize(final int scanBatchSize) {
        this.scanBatchSize = scanBatchSize;
    }

    public int getReadRangeSize() {
        return readRangeSize;
    }

    public void setReadRangeSize(final int readRangeSize) {
        this.readRangeSize = readRangeSize;
    }

    public int getWriteChunkSize() {
        return writeChunkSize;
    }

    public void setWriteChunkSize(final int writeChunkSize) {
        this.writeChunkSize = writeChunkSize;
    }

}
