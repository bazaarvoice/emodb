package com.bazaarvoice.emodb.blob.db.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.util.IOUtils;
import com.bazaarvoice.emodb.blob.api.Range;
import com.bazaarvoice.emodb.blob.api.StreamSupplier;
import com.bazaarvoice.emodb.blob.core.BlobPlacement;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.ByteStreams;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class S3StorageProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3StorageProvider.class);

    private final PlacementFactory _placementFactory;
    private final LoadingCache<String, BlobPlacement> _placementCache;

    @Inject
    public S3StorageProvider(final PlacementFactory placementFactory) {
        _placementFactory = Objects.requireNonNull(placementFactory);
        _placementCache = CacheBuilder.newBuilder().build(new CacheLoader<String, BlobPlacement>() {
            @Override
            public BlobPlacement load(@Nonnull final String placement) throws ConnectionException {
                BlobPlacement blobPlacement = (BlobPlacement) _placementFactory.newPlacement(placement);
                LOGGER.debug("Blob placement: {}", blobPlacement);
                return blobPlacement;
            }
        });
    }

    private static AmazonS3URI getAmazonS3URI(final String bucket, final String tableName, @Nullable final String blobId) {
        final String key = null == blobId ? tableName : tableName + "/" + blobId;
        return new AmazonS3URI(String.format("s3://%s/%s", bucket, key));
    }

    private static S3Object getObject(final AmazonS3 s3Client, final AmazonS3URI uri, @Nullable final Range range) {
        final GetObjectRequest rangeObjectRequest = new GetObjectRequest(uri.getBucket(), uri.getKey());

        if (null != range) {
            rangeObjectRequest
                    .withRange(range.getOffset(), range.getOffset() + range.getLength());
        }

        try {
            LOGGER.debug("Get S3 object: {}, range: {}", uri, rangeObjectRequest.getRange());
            return s3Client.getObject(rangeObjectRequest);
        } catch (final AmazonS3Exception e) {
            LOGGER.error("Failed to get S3 object: {}, range: {}", uri, range);
            throw new RuntimeException(e);
        }
    }

    public StreamSupplier getObjectStreamSupplier(final String tableName, final String tablePlacement, final String blobId, @Nullable final Range range) {
        BlobPlacement blobPlacement = _placementCache.getUnchecked(tablePlacement);
        S3BucketConfiguration s3BucketConfiguration = blobPlacement.getS3BucketConfiguration();
        AmazonS3 s3Client = blobPlacement.getS3Client();

        return out -> toSubRanges(range, s3BucketConfiguration.getReadRangeSize())
                .parallelStream()
                .forEachOrdered(subrange -> {
                    final AmazonS3URI uri = getAmazonS3URI(blobPlacement.getS3BucketConfiguration().getName(), tableName, blobId);
                    final S3Object object = getObject(s3Client, uri, subrange);
                    writeTo(object.getObjectContent(), out);
                });
    }

    /**
     * Splits range into disjoint subranges
     *
     * @param range
     * @param subRangeSize
     * @return
     */
    @VisibleForTesting
    protected static List<Range> toSubRanges(final Range range, final long subRangeSize) {
        Objects.requireNonNull(range);
        if (1 > subRangeSize) {
            throw new IllegalArgumentException("Range size should be >= 1");
        }
        final List<Range> subRanges = new ArrayList<>();
        if (range.getLength() > subRangeSize) {
            long max = range.getLength() + range.getOffset();
            int j = 0;
            while (j * subRangeSize < range.getLength()) {
                long offset = range.getOffset() + j * (subRangeSize + 1);
                long length = Math.min(max - offset, subRangeSize);
                if (0 == length) {
                    break;
                }
                subRanges.add(new Range(offset, length));
                j++;
            }
        } else {
            //put null to not create range request
            subRanges.add(null);
        }
        LOGGER.debug("Range: {} -> subranges: {}", range, subRanges);
        return subRanges;
    }

    private static void writeTo(final InputStream input, final OutputStream out) {
        try {
            IOUtils.copy(input, out);
        } catch (final IOException e) {
            new RuntimeException(e);
        } finally {
            try {
                input.close();
                out.flush();
            } catch (final IOException e) {
                new RuntimeException(e);
            }
        }
    }

    public ObjectMetadata putObject(final String tableName, final String tablePlacement, final String blobId, final InputStream input, final Map<String, String> attributes) {
//        The S3 API requires that a content length be set before starting uploading,
//        which is a problem when you want to calculate a large amount of data on the fly.
//        The standard Java AWS SDK will simply buffer all the data in memory so that it can calculate the length,
//        which consumes RAM and delays the upload.
//        We can write the data to a temporary file but disk IO is slow.
        BlobPlacement blobPlacement = _placementCache.getUnchecked(tablePlacement);
        S3BucketConfiguration s3BucketConfiguration = blobPlacement.getS3BucketConfiguration();
        final AmazonS3URI uri = getAmazonS3URI(s3BucketConfiguration.getName(), tableName, blobId);
        final AmazonS3 s3Client = blobPlacement.getS3Client();

        final byte[] bytes = new byte[s3BucketConfiguration.getWriteChunkSize()];

        final int partSize;
        try {
            partSize = ByteStreams.read(input, bytes, 0, bytes.length);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final ObjectMetadata objectMetadata;
        if (partSize < bytes.length) {
            //If stream size less than DEFAULT_WRITE_CHUNK_SIZE,
            // S3 object and metadata cane be uploaded in one request
            final byte[] buf = Arrays.copyOf(bytes, partSize);
            final MessageDigest mdMD5 = getMessageDigest("MD5");
            mdMD5.update(bytes);
            final String md5 = Hex.encodeHexString(mdMD5.digest());

            final MessageDigest mdSHA1 = getMessageDigest("SHA-1");
            mdSHA1.update(bytes);
            final String sha1 = Hex.encodeHexString(mdSHA1.digest());

            objectMetadata = getObjectMetadata(sha1, md5, buf.length, attributes);

            putObjectKnownSize(s3Client, uri, new ByteArrayInputStream(buf), objectMetadata);
            return objectMetadata;
        } else {
            // otherwise, upload stream via multipart
            objectMetadata = putObjectUnknownSize(s3Client, uri, bytes, input, s3BucketConfiguration.getWriteChunkSize(), attributes);
        }
        return objectMetadata;
    }

    private static ObjectMetadata putObjectUnknownSize(final AmazonS3 amazonS3, final AmazonS3URI uri, final byte[] firstPart, final InputStream lastPart,
                                                       int writeChunkSize, final Map<String, String> metadataAttributes) {
        int partNumber = 1;
        int partSize;
        long contentLength = 0;
        LOGGER.debug("Started to put S3 object unknown size, uri: {}", uri);
        // Initiate the multipart upload.
        final InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(uri.getBucket(), uri.getKey());
        final InitiateMultipartUploadResult initResponse = amazonS3.initiateMultipartUpload(initRequest);

        UploadPartRequest uploadRequest = new UploadPartRequest()
                .withBucketName(uri.getBucket())
                .withKey(uri.getKey())
                .withUploadId(initResponse.getUploadId())
                .withInputStream(new ByteArrayInputStream(firstPart))
                .withPartNumber(partNumber)
                .withPartSize(firstPart.length);

        UploadPartResult uploadResult = amazonS3.uploadPart(uploadRequest);
        contentLength += firstPart.length;
        LOGGER.debug("Uploaded S3 object part, uri: {}, part: {}", uri, partNumber);

        // Create a list of ETag objects. You retrieve ETags for each object part uploaded,
        // then, after each individual part has been uploaded, pass the list of ETags to
        // the request to complete the upload.
        final List<PartETag> partETags = new ArrayList<>();
        partETags.add(uploadResult.getPartETag());

        byte[] bytes = new byte[writeChunkSize];
        final DigestInputStream md5In = new DigestInputStream(lastPart, getMessageDigest("MD5"));
        final DigestInputStream sha1In = new DigestInputStream(md5In, getMessageDigest("SHA-1"));
        for (; ; ) {
            try {
                partSize = ByteStreams.read(sha1In, bytes, 0, writeChunkSize);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
            if (partSize == 0) {
                break;
            }
            try {
                final ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, partSize);
                ++partNumber;
                // Create the request to upload a part.
                uploadRequest = new UploadPartRequest()
                        .withBucketName(uri.getBucket())
                        .withKey(uri.getKey())
                        .withUploadId(initResponse.getUploadId())
                        .withInputStream(new ByteArrayInputStream(buffer.array()))
                        .withPartNumber(partNumber)
                        .withPartSize(partSize)
                        .withLastPart(partSize < bytes.length);

                // Upload the part and add the response's ETag to our list.
                uploadResult = amazonS3.uploadPart(uploadRequest);
                contentLength += partSize;
                partETags.add(uploadResult.getPartETag());
                LOGGER.debug("Uploaded S3 object part, uri: {}, part: {}", uri, partNumber);
            } catch (final Exception ex) {
                LOGGER.error("Failed to put S3 object part, uri: {}, part: {}", uri, partNumber);
                throw new RuntimeException("Error on file split.", ex);
            }
        }

        // Complete the multipart upload.
        final CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(uri.getBucket(), uri.getKey(),
                initResponse.getUploadId(), partETags);

        amazonS3.completeMultipartUpload(compRequest);

        // Include two types of hash: md5 (because it's common) and sha1 (because it's secure)
        final String md5 = Hex.encodeHexString(md5In.getMessageDigest().digest());
        final String sha1 = Hex.encodeHexString(sha1In.getMessageDigest().digest());

        final ObjectMetadata om = getObjectMetadata(md5, sha1, contentLength, metadataAttributes);
        ObjectMetadata objectMetadata = overwriteObjectMetadata(amazonS3, uri, om);
        LOGGER.debug("S3 object has been uploaded, uri: {}, length: {}", uri, om.getContentLength());
        return objectMetadata;
    }

    private static ObjectMetadata getObjectMetadata(final String md5, final String sha1, long contentLenght, final Map<String, String> attributes) {
        final ObjectMetadata om = new ObjectMetadata();
        attributes.put("sha-1", sha1);
        attributes.put("md5", md5);
        if (null != attributes.get("contentType")) {
            om.setContentType(attributes.remove("contentType"));
        }
        om.setUserMetadata(attributes);
        om.setLastModified(new Date());
        om.setContentLength(contentLenght);
        return om;
    }

    private static ObjectMetadata overwriteObjectMetadata(final AmazonS3 amazonS3, final AmazonS3URI uri, final ObjectMetadata objectMetadata) {
        final CopyObjectRequest request = new CopyObjectRequest(uri.getBucket(), uri.getKey(), uri.getBucket(), uri.getKey())
                .withNewObjectMetadata(objectMetadata);

        try {
            LOGGER.debug("Copy S3 object, uri: {}, metadata: {}", uri, objectMetadata.getRawMetadata());
            CopyObjectResult copyObjectResult = amazonS3.copyObject(request);
            objectMetadata.setLastModified(copyObjectResult.getLastModifiedDate());

            return objectMetadata;
        } catch (final AmazonS3Exception e) {
            LOGGER.error("Failed to overwrite S3 object metadata: uri: {}, metadata: {}", uri, objectMetadata.getRawMetadata());
            throw new RuntimeException(e);
        }
    }

    private static void putObjectKnownSize(final AmazonS3 amazonS3, final AmazonS3URI uri, final InputStream input, final ObjectMetadata om) {
        final PutObjectRequest putObjectRequest = new PutObjectRequest(uri.getBucket(), uri.getKey(), input, om);
        try {
            LOGGER.debug("Put object known size, uri: {}", uri);
            amazonS3.putObject(putObjectRequest);
            LOGGER.debug("S3 object has been uploaded, uri: {}, length: {}", uri, om.getContentLength());
        } catch (final AmazonS3Exception e) {
            LOGGER.error("Failed to put S3 object: {}", uri);
            throw new RuntimeException(e);
        }
    }

    private static MessageDigest getMessageDigest(final String algorithmName) {
        try {
            return MessageDigest.getInstance(algorithmName);
        } catch (final NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private Stream<AmazonS3URI> list(final String tableName, final String tablePlacement, @Nullable final String fromBlobIdExclusive, final long limit) {
        BlobPlacement blobPlacement = _placementCache.getUnchecked(tablePlacement);
        S3BucketConfiguration s3BucketConfiguration = blobPlacement.getS3BucketConfiguration();

        final AmazonS3URI uri = getAmazonS3URI(s3BucketConfiguration.getName(), tableName, null);
        LOGGER.debug("List table uri: {}", uri);

        final AmazonS3 s3Client = blobPlacement.getS3Client();

        Stream<S3ObjectSummary> summaryStream = Stream.<S3ObjectSummary>builder().build();

        final ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(uri.getBucket())
                .withPrefix(uri.getKey())
                .withStartAfter(fromBlobIdExclusive)
                .withMaxKeys(Math.toIntExact(Math.min(limit, s3BucketConfiguration.getScanBatchSize())));
        ListObjectsV2Result result;
        do {
            result = s3Client.listObjectsV2(request);
            summaryStream = Stream.concat(summaryStream, result.getObjectSummaries().parallelStream());

            // If there are more than maxKeys keys in the bucket, get a continuation token
            // and list the next objects.
            final String token = result.getNextContinuationToken();
            request.setContinuationToken(token);
        } while (result.isTruncated());

        return summaryStream.parallel()
                .filter(s3ObjectSummary -> !(uri.getKey() + "/").equals(s3ObjectSummary.getKey()))
                .map(s3ObjectSummary -> new AmazonS3URI(String.format("s3://%s/%s", uri.getBucket(), s3ObjectSummary.getKey())))
                .limit(limit);
    }

    //TODO verify if needed
    private void deleteTable(final String tableName, final String tablePlacement) {
        boolean withVersions = false;
//        TODO consider to use S3 Batch operations
        BlobPlacement blobPlacement = _placementCache.getUnchecked(tablePlacement);
        S3BucketConfiguration s3BucketConfiguration = blobPlacement.getS3BucketConfiguration();
        final AmazonS3URI uri = getAmazonS3URI(s3BucketConfiguration.getName(), tableName, null);
        LOGGER.debug("Delete table uri: {}", uri);

        final AmazonS3 s3Client = blobPlacement.getS3Client();

        final AtomicInteger counter = new AtomicInteger();
        list(tableName, tablePlacement, null, Long.MAX_VALUE)
                .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / s3BucketConfiguration.getScanBatchSize()))
                .forEach((integer, uris) -> delete(s3Client, uris, withVersions));
        deleteObject(s3Client, uri);
        LOGGER.debug("Table has been deleted uri: {}", uri);
    }

    public void deleteObject(final String tableName, final String tablePlacement, final String blobId) {
        BlobPlacement blobPlacement = _placementCache.getUnchecked(tablePlacement);
        final AmazonS3URI uri = getAmazonS3URI(blobPlacement.getS3BucketConfiguration().getName(), tableName, blobId);
        final AmazonS3 s3Client = blobPlacement.getS3Client();
        delete(s3Client, Collections.singletonList(uri), false);
    }

    private void delete(final AmazonS3 s3Client, final List<AmazonS3URI> uris, boolean withVersions) {
        //TODO consider to use Futures
        uris.forEach(uri -> deleteObject(s3Client, uri, withVersions));
    }

    private static void deleteObject(final AmazonS3 s3Client, final AmazonS3URI uri, boolean withVersions) {
        try {
            LOGGER.debug("Starting to delete S3 object uri: {}", uri);
            deleteObject(s3Client, uri);
            if (withVersions) {
                deleteVersions(s3Client, uri);
            }
            LOGGER.debug("S3 object has been deleted uri: {}", uri);
        } catch (final AmazonS3Exception e) {
            LOGGER.error("Failed to delete S3 object: {}", uri);
            throw new RuntimeException(e);
        }
    }

    private static void deleteVersions(AmazonS3 s3Client, AmazonS3URI uri) {
        final DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(uri.getBucket())
                .withKeys(uri.getKey());
        final DeleteObjectsResult deleteObjectsResult = s3Client.deleteObjects(deleteObjectsRequest);

        final List<DeleteObjectsRequest.KeyVersion> keyVersionsMarkers = deleteObjectsResult.getDeletedObjects().stream()
                .map(deletedObject -> new DeleteObjectsRequest.KeyVersion(deletedObject.getKey(), deletedObject.getDeleteMarkerVersionId()))
                .collect(Collectors.toList());
        s3Client.deleteObjects(new DeleteObjectsRequest(uri.getBucket())
                .withKeys(keyVersionsMarkers));
    }

    private static void deleteObject(AmazonS3 s3Client, AmazonS3URI uri) {
        try {
            s3Client.deleteObject(new DeleteObjectRequest(uri.getBucket(), uri.getKey()));
        } catch (final AmazonS3Exception e) {
            LOGGER.error("Failed to delete S3 object: {}", uri);
            throw new RuntimeException(e);
        }
    }
}
