package com.bazaarvoice.emodb.blob.db.s3;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class AmazonS3Provider {
//    TODO rewrite to factory method
    public static AmazonS3 getAmazonS3(final S3BucketConfiguration s3BucketConfiguration) {
        AmazonS3ClientBuilder amazonS3ClientBuilder = AmazonS3ClientBuilder.standard()
                .withCredentials(getAwsCredentialsProvider(s3BucketConfiguration))
                .withAccelerateModeEnabled(s3BucketConfiguration.getAccelerateModeEnabled());
        if (null != s3BucketConfiguration.getRegion()) {
            amazonS3ClientBuilder
                    .withRegion(Regions.fromName(s3BucketConfiguration.getRegion()));
        } else if (null != s3BucketConfiguration.getS3ClientConfiguration()) {
            S3ClientConfiguration.EndpointConfiguration endpointConfiguration = s3BucketConfiguration.getS3ClientConfiguration().getEndpointConfiguration();
            amazonS3ClientBuilder
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpointConfiguration.getServiceEndpoint(), endpointConfiguration.getSigningRegion()));
        }
        return amazonS3ClientBuilder
                .build();
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
