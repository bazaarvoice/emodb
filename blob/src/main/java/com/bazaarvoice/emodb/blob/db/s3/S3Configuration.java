package com.bazaarvoice.emodb.blob.db.s3;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;

public class S3Configuration {
    @Valid
    @NotNull
    @JsonProperty("buckets")
    private List<S3BucketConfiguration> _s3BucketConfigurations = new ArrayList<>();

    @Valid
    @JsonProperty("client")
    private S3ClientConfiguration _s3ClientConfiguration;

    public List<S3BucketConfiguration> getS3BucketConfigurations() {
        return _s3BucketConfigurations;
    }

    public S3Configuration setS3BucketConfigurations(final List<S3BucketConfiguration> s3BucketConfigurations) {
        _s3BucketConfigurations = s3BucketConfigurations;
        return this;
    }

    public S3ClientConfiguration getS3ClientConfiguration() {
        return _s3ClientConfiguration;
    }

    public void setS3ClientConfiguration(final S3ClientConfiguration s3ClientConfiguration) {
        _s3ClientConfiguration = s3ClientConfiguration;
    }
}
