package com.bazaarvoice.emodb.common.stash;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;

import java.net.URI;

/**
 * Provides basic access to Stashed tables and content from a fixed top-level directory.  Unlike with
 * StandardStashReader table directories are immediate subdirectories of the root.
 */
public class FixedStashReader extends StashReader {

    public static FixedStashReader getInstance(URI stashRoot) {
        return getInstance(stashRoot, new DefaultAWSCredentialsProviderChain());
    }

    public static FixedStashReader getInstance(URI stashRoot, String accessKey, String secretKey) {
        return getInstance(stashRoot, new StaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)));
    }

    public static FixedStashReader getInstance(URI stashRoot, AWSCredentialsProvider credentialsProvider) {
        AmazonS3 s3 = getS3Client(stashRoot, credentialsProvider);
        return new FixedStashReader(stashRoot, s3);
    }

    public static FixedStashReader getInstance(URI stashRoot, AmazonS3 s3) {
        return new FixedStashReader(stashRoot, s3);
    }

    FixedStashReader(URI stashRoot, AmazonS3 s3) {
        super(stashRoot, s3);
    }

    @Override
    protected String getRootPath() {
        // For a fixed reader the root path is constant
        return _rootPath;
    }
}
