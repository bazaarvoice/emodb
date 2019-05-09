package com.bazaarvoice.emodb.blob.db.s3;

public class S3BucketConfiguration {
    private String name;
    private String region;
    private String roleArn;
    private String roleExternalId;
    private Boolean accelerateModeEnabled;
    private S3ClientConfiguration s3ClientConfiguration;

    public S3BucketConfiguration(final String name, final String region, final String roleArn, final String roleExternalId, Boolean accelerateModeEnabled, S3ClientConfiguration s3ClientConfiguration) {
        this.name = name;
        this.region = region;
        this.roleArn = roleArn;
        this.roleExternalId = roleExternalId;
        this.accelerateModeEnabled = accelerateModeEnabled;
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

}
