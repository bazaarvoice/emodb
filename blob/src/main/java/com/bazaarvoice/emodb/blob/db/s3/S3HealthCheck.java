package com.bazaarvoice.emodb.blob.db.s3;

import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.bazaarvoice.emodb.blob.db.s3.config.S3HealthCheckConfiguration;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.inject.Inject;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class S3HealthCheck extends HealthCheck {
    private final AmazonS3Provider _amazonS3Provider;
    private final S3HealthCheckConfiguration _healthCheckConfiguration;
    private final LoadingCache<String, Result> _healthCheckCache;

    @Inject
    public S3HealthCheck(AmazonS3Provider amazonS3Provider,
                         S3HealthCheckConfiguration s3HealthCheckConfiguration,
                         HealthCheckRegistry healthCheckRegistry) {
        _amazonS3Provider = Objects.requireNonNull(amazonS3Provider);
        _healthCheckConfiguration = Objects.requireNonNull(s3HealthCheckConfiguration);
        _healthCheckCache = CacheBuilder.newBuilder()
                .expireAfterWrite(_healthCheckConfiguration.getDuration().getSeconds(), TimeUnit.SECONDS)
                .build(new CacheLoader<String, Result>() {
                    @Override
                    public Result load(String key) throws Exception {
                        return getResult();
                    }
                });

        healthCheckRegistry.addHealthCheck(_healthCheckConfiguration.getName(), this);
    }

    @Override
    protected Result check() throws Exception {
        return _healthCheckCache.getUnchecked(_healthCheckConfiguration.getName());
    }

    private Result getResult() {
        return _amazonS3Provider.getS3BucketNamesToS3Clients().entrySet().parallelStream()
                .map(entry -> {
                    try {
                        entry.getValue().listObjectsV2(new ListObjectsV2Request()
                                .withBucketName(entry.getKey())
                                .withMaxKeys(1));
                        return Result.healthy("Bucket " + entry.getKey() + " is healthy");
                    } catch (Exception e) {
                        return Result.unhealthy("Bucket " + entry.getKey() + " is unhealthy: " + e.getMessage());
                    }
                }).reduce((result1, result2) -> {
                    String message = result1.getMessage() + ", " + result2.getMessage();
                    if (result1.isHealthy() && result2.isHealthy()) {
                        return Result.healthy(message);
                    } else {
                        return Result.unhealthy(message);
                    }
                })
                .orElse(Result.healthy("No s3 buckets configured"));
    }
}
