package com.bazaarvoice.emodb.blob.db.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.bazaarvoice.emodb.blob.db.s3.config.S3BucketConfiguration;
import com.bazaarvoice.emodb.blob.db.s3.config.S3ClientConfiguration;
import com.bazaarvoice.emodb.blob.db.s3.config.S3Configuration;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class AmazonS3Provider {

    private final Map<String, AmazonS3> _s3BucketNamesToS3Clients;

    @Inject
    public AmazonS3Provider(@Nullable S3Configuration s3Configuration, Clock clock) {
        _s3BucketNamesToS3Clients = getS3BucketNamesToS3Clients(s3Configuration, clock);
    }

    private Map<String, AmazonS3> getS3BucketNamesToS3Clients(S3Configuration s3Configuration, Clock clock) {
        //    TODO remove condition in EMO-7107
        if (null != s3Configuration && null != s3Configuration.getS3BucketConfigurations()) {
            return s3Configuration.getS3BucketConfigurations().stream()
                    .collect(Collectors.toMap(
                            s3BucketConfiguration -> s3BucketConfiguration.getName(),
                            s3BucketConfiguration -> getAmazonS3(s3BucketConfiguration, clock))
                    );
        } else {
            return new HashMap<>();
        }
    }

    public AmazonS3 get(String bucket) {
        return _s3BucketNamesToS3Clients.get(bucket);
    }

    Map<String, AmazonS3> getS3BucketNamesToS3Clients() {
        return _s3BucketNamesToS3Clients;
    }

    private static AmazonS3 getAmazonS3(final S3BucketConfiguration s3BucketConfiguration, Clock clock) {
        AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard()
                .withCredentials(getAwsCredentialsProvider(s3BucketConfiguration))
                .withAccelerateModeEnabled(s3BucketConfiguration.getAccelerateModeEnabled());
        S3ClientConfiguration.RateLimitConfiguration rateLimitConfiguration = new S3ClientConfiguration.RateLimitConfiguration();
        if (null != s3BucketConfiguration.getRegion()) {
            amazonS3ClientBuilder
                    .withRegion(Regions.fromName(s3BucketConfiguration.getRegion()));
        } else if (null != s3BucketConfiguration.getS3ClientConfiguration()) {
            S3ClientConfiguration.EndpointConfiguration endpointConfiguration = s3BucketConfiguration.getS3ClientConfiguration().getEndpointConfiguration();
            amazonS3ClientBuilder
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpointConfiguration.getServiceEndpoint(), endpointConfiguration.getSigningRegion()));
            rateLimitConfiguration = s3BucketConfiguration.getS3ClientConfiguration().getRateLimitConfiguration();
        }
        AmazonS3 amazonS3 = amazonS3ClientBuilder
                .build();

        return new S3RateLimiter(clock, rateLimitConfiguration)
                .rateLimit(amazonS3);
    }

    private static AWSCredentialsProvider getAwsCredentialsProvider(final S3BucketConfiguration s3BucketConfiguration) {
        final AWSCredentialsProvider credentialsProvider;
        if (null != s3BucketConfiguration.getRoleArn()) {
            final STSAssumeRoleSessionCredentialsProvider.Builder credentialsProviderBuilder = new STSAssumeRoleSessionCredentialsProvider.Builder(s3BucketConfiguration.getRoleArn(), s3BucketConfiguration.getName() + "session");
            if (null != s3BucketConfiguration.getRoleExternalId()) {
                credentialsProviderBuilder.withExternalId(s3BucketConfiguration.getRoleExternalId());
            }
            credentialsProvider = credentialsProviderBuilder.build();
        } else {
            credentialsProvider = new DefaultAWSCredentialsProviderChain();
        }
        return credentialsProvider;
    }
}
