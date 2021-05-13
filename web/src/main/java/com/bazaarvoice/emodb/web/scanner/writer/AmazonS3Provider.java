package com.bazaarvoice.emodb.web.scanner.writer;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;

import javax.ws.rs.core.Response;

/**
 * Provides {@link AmazonS3} client's configured for the same region as an S3 bucket.  The clients are cached between
 * requests.
 */
public class AmazonS3Provider {

    private AWSCredentialsProvider _credentialsProvider;
    private String _localRegionName;

    private final LoadingCache<String, AmazonS3> _clientsByRegion;
    private final Cache<String, AmazonS3> _clientsByBucket;

    @Inject
    public AmazonS3Provider(@S3CredentialsProvider AWSCredentialsProvider credentialsProvider, Region region) {
        this(credentialsProvider, region.getName());
    }

    @VisibleForTesting
    public AmazonS3Provider(AWSCredentialsProvider credentialsProvider, String localRegionName) {
        _credentialsProvider = credentialsProvider;
        _localRegionName = localRegionName;

        _clientsByRegion = CacheBuilder.newBuilder()
                .maximumSize(4)
                .build(new CacheLoader<String, AmazonS3>() {
                    @Override
                    public AmazonS3 load(String regionName) throws Exception {
                        return createS3ClientForRegion(regionName);
                    }
                });

        _clientsByBucket = CacheBuilder.newBuilder()
                .maximumSize(10)
                .build();
    }

    public AmazonS3 getLocalS3Client() {
        return _clientsByRegion.getUnchecked(_localRegionName);
    }

    public AmazonS3 getS3ClientForBucket(String bucket) {
        AmazonS3 client = _clientsByBucket.getIfPresent(bucket);
        
        if (client == null) {
            String regionName = getRegionForBucket(bucket);

            if (regionName == null) {
                // If the bucket does not exist then just use the current region.  It really doesn't matter which client
                // we use since the bucket doesn't exist anywhere.  But don't cache the client in case the bucket gets
                // created between now and a future request.
                return getLocalS3Client();
            }

            client = _clientsByRegion.getUnchecked(regionName);
            _clientsByBucket.put(bucket, client);
        }

        return client;
    }

    /**
     * The version of the AWS SDK Emo uses doesn't have support for some of the newer Amazon regions, such as
     * "us-east-2" and "eu-west-2".  This leads to unexpected errors when accessing to an S3 bucket in one of these
     * regions.  The work-around is to use {@link AmazonS3#setEndpoint(String)} instead of the simpler
     * {@link AmazonS3#setRegion(Region)} since this doesn't run into the same issues.  The only downside is the
     * necessity to code in the s3 endpoint pattern, but this is extremely unlikely to change.
     */
    public AmazonS3 createS3ClientForRegion(String regionName) {
        AmazonS3  s3;
        return AmazonS3ClientBuilder.standard().withCredentials(_credentialsProvider)
                .withRegion(regionName).build();
    }

    public String getRegionForBucket(String bucket) {
        // Just querying for the location for a bucket can be done with the local client
        AmazonS3 client = getLocalS3Client();
        try {
            String region = client.getBucketLocation(bucket);
            if ("US".equals(region)) {
                // GetBucketLocation requests return null for us-east-1 which the SDK then replaces with "US".
                // So change it to the actual region.
                region = "us-east-1";
            }
            return region;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                // If the bucket doesn't exist then return null
                return null;
            }
            throw e;
        }
    }
}
