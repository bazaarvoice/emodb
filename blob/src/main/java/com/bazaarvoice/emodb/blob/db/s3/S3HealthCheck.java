package com.bazaarvoice.emodb.blob.db.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.codahale.metrics.health.HealthCheck;

import javax.inject.Inject;
import java.util.Map;
import java.util.Objects;

public class S3HealthCheck extends HealthCheck {
    private final Map<String, AmazonS3> _bucketNamesToS3Clients;

    @Inject
    public S3HealthCheck(@S3BucketNamesToS3Clients Map<String, AmazonS3> bucketNamesToS3Clients,
                         HealthCheckRegistry healthCheckRegistry) {
        _bucketNamesToS3Clients = Objects.requireNonNull(bucketNamesToS3Clients);
        healthCheckRegistry.addHealthCheck("blob-s3", this);
    }

    @Override
    protected Result check() throws Exception {
        return _bucketNamesToS3Clients.entrySet().parallelStream()
                .map(entry -> {
                    try {
                        entry.getValue().listObjectsV2(new ListObjectsV2Request()
                                .withBucketName(entry.getKey())
                                .withMaxKeys(1));
                        return Result.healthy("Bucket " + entry.getKey() + " is healthy");
                    } catch (Exception e) {
                        return Result.unhealthy("Bucket " + entry.getKey() + " is unhealthy: " + e.getMessage());
                    }
                }).reduce((result, result2) -> {
                    String message = result.getMessage() + ", " + result2.getMessage();
                    if (result.isHealthy() && result2.isHealthy()) {
                        return Result.healthy(message);
                    } else {
                        return Result.unhealthy(message);
                    }
                })
                .orElse(Result.healthy("No s3 buckets configured"));
    }
}
