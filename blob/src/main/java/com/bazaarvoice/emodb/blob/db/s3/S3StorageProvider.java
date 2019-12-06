package com.bazaarvoice.emodb.blob.db.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.ByteStreams;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.core.Response;
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
            public BlobPlacement load(final String placement) throws ConnectionException {
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

    public S3Object getObject(final BlobPlacement blobPlacement, final String tableName, final String blobId, @Nullable final Range range) {
        final AmazonS3URI uri = getAmazonS3URI(blobPlacement.getS3BucketConfiguration().getName(), tableName, blobId);
        final GetObjectRequest rangeObjectRequest = new GetObjectRequest(uri.getBucket(), uri.getKey());

        if (null != range) {
            rangeObjectRequest
                    .withRange(range.getOffset(), range.getOffset() + range.getLength());
        }

        try {
            LOGGER.debug("Get s3 object: {}, range: {}", uri, range);
            return blobPlacement.getS3Client()
                    .getObject(rangeObjectRequest);
        } catch (final AmazonS3Exception e) {
            LOGGER.error("Failed to get s3 object: {}, range: {}", uri, range);
            throw new RuntimeException(e);
        }
    }

    public StreamSupplier getObjectStreamSupplier(final String tableName, final String tablePlacement, final String blobId, @Nullable final Range range) {
        BlobPlacement blobPlacement = _placementCache.getUnchecked(tablePlacement);
        S3BucketConfiguration s3BucketConfiguration = blobPlacement.getS3BucketConfiguration();

        return out -> toSubRanges(range, s3BucketConfiguration.getReadRangeSize())
                .parallelStream()
                .forEachOrdered(subrange -> {
                    final S3Object object = getObject(blobPlacement, tableName, blobId, subrange);
                    LOGGER.debug("Get blob: {}, range: {}", object, subrange);
                    writeTo(object.getObjectContent(), out);
                });
    }

    /**
     * Splits range into disjoint subranges
     *
     * @param range
     * @param rangeSize
     * @return
     */
    private static List<Range> toSubRanges(final Range range, final long rangeSize) {
        Objects.requireNonNull(range);
        if (1 >= rangeSize) {
            throw new IllegalArgumentException("Range size should be >= 1");
        }
        final List<Range> subRanges = new ArrayList<>();

        if (range.getLength() > rangeSize) {
            int j = 0;
            long max = range.getLength() + range.getOffset();
            while (j * rangeSize < range.getLength()) {
                long offset = range.getOffset() + j * rangeSize;
                long suffix = max - offset;
                long length = suffix >= 0 && suffix < rangeSize ? suffix : rangeSize;
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

    public ObjectMetadata getObjectMetadata(final String tableName, final String tablePlacement, final String blobId) {
        BlobPlacement blobPlacement = _placementCache.getUnchecked(tablePlacement);
        final AmazonS3URI uri = getAmazonS3URI(blobPlacement.getS3BucketConfiguration().getName(), tableName, blobId);
        final AmazonS3 s3Client = blobPlacement.getS3Client();

        ObjectMetadata metadata;
        try {
            LOGGER.debug("Get s3 object metadata: {}", uri);
            metadata = s3Client.getObjectMetadata(uri.getBucket(), uri.getKey());
        } catch (final AmazonS3Exception e) {
            if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                metadata = null;
            } else {
                LOGGER.error("Failed to get s3 object metadata: {}", uri);
                throw new RuntimeException(e);
            }
        }
        return metadata;
    }

    public void putObject(final String tableName, final String tablePlacement, final String blobId, final InputStream input, final Map<String, String> attributes) {
//        The S3 API requires that a content length be set before starting uploading,
//        which is a problem when you want to calculate a large amount of data on the fly.
//        The standard Java AWS SDK will simply buffer all the data in memory so that it can calculate the length,
//        which consumes RAM and delays the upload.
//        We can write the data to a temporary file but disk IO is slow.
        BlobPlacement blobPlacement = _placementCache.getUnchecked(tablePlacement);
        S3BucketConfiguration s3BucketConfiguration = blobPlacement.getS3BucketConfiguration();
        final AmazonS3URI uri = getAmazonS3URI(s3BucketConfiguration.getName(), tableName, blobId);
        final AmazonS3 writeS3Client = blobPlacement.getS3Client();

        final byte[] bytes = new byte[s3BucketConfiguration.getWriteChunkSize()];

        final int partSize;
        try {
            partSize = ByteStreams.read(input, bytes, 0, bytes.length);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }

        if (partSize < bytes.length) {
            //If stream size less than DEFAULT_WRITE_CHUNK_SIZE,
            // s3 object and metadata cane be uploaded in one request
            final byte[] buf = Arrays.copyOf(bytes, partSize);
            final MessageDigest mdMD5 = getMessageDigest("MD5");
            mdMD5.update(bytes);
            final String md5 = Hex.encodeHexString(mdMD5.digest());

            final MessageDigest mdSHA1 = getMessageDigest("SHA-1");
            mdSHA1.update(bytes);
            final String sha1 = Hex.encodeHexString(mdSHA1.digest());

            final ObjectMetadata objectMetadata = getObjectMetadata(sha1, md5, attributes);
            objectMetadata.setContentLength(buf.length);

            putObjectKnownSize(writeS3Client, uri, new ByteArrayInputStream(buf), objectMetadata);
        } else {
            // otherwise, upload stream via multipart
            putObjectUnknownSize(writeS3Client, uri, bytes, input, s3BucketConfiguration.getWriteChunkSize(), attributes);
        }
    }

    private void putObjectUnknownSize(final AmazonS3 amazonS3, final AmazonS3URI uri, final byte[] firstPart, final InputStream lastPart,
                                      int writeChunkSize, final Map<String, String> metadataAttributes) {
        int partNumber = 1;
        int partSize;

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
                partETags.add(uploadResult.getPartETag());
            } catch (final Exception ex) {
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

        final ObjectMetadata om = getObjectMetadata(md5, sha1, metadataAttributes);
        overwriteObjectMetadata(amazonS3, uri, om);
    }

    private ObjectMetadata getObjectMetadata(final String md5, final String sha1, final Map<String, String> attributes) {
        final ObjectMetadata om = new ObjectMetadata();
        attributes.put("SHA-1", sha1);
        attributes.put("MD5", md5);
        if (null != attributes.get("contentType")) {
            om.setContentType(attributes.remove("contentType"));
        }
        om.setUserMetadata(attributes);
        return om;
    }

    private void overwriteObjectMetadata(final AmazonS3 amazonS3, final AmazonS3URI uri, final ObjectMetadata objectMetadata) {
        final CopyObjectRequest request = new CopyObjectRequest(uri.getBucket(), uri.getKey(), uri.getBucket(), uri.getKey())
                .withNewObjectMetadata(objectMetadata);

        try {
            LOGGER.debug("Copy s3 object, uri: {}, metadata: {}", uri, objectMetadata.getRawMetadata());
            amazonS3.copyObject(request);
        } catch (final AmazonS3Exception e) {
            LOGGER.error("Failed to overwrite s3 object metadata: uri: {}, metadata: {}", uri, objectMetadata.getRawMetadata());
            throw new RuntimeException(e);
        }
    }

    private void putObjectKnownSize(final AmazonS3 amazonS3, final AmazonS3URI uri, final InputStream input, final ObjectMetadata objectMetadata) {
        final PutObjectRequest putObjectRequest = new PutObjectRequest(uri.getBucket(), uri.getKey(), input, objectMetadata);
        try {
            LOGGER.debug("Put object known size, uri: {}, metadata: {}", uri, objectMetadata.getRawMetadata());
            amazonS3.putObject(putObjectRequest);
        } catch (final AmazonS3Exception e) {
            LOGGER.error("Failed to put s3 object: {}", uri);
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

    public Stream<S3ObjectSummary> list(final String tableName, final String tablePlacement, @Nullable final String fromBlobIdExclusive, final long limit) {
        BlobPlacement blobPlacement = _placementCache.getUnchecked(tablePlacement);
        S3BucketConfiguration s3BucketConfiguration = blobPlacement.getS3BucketConfiguration();

        final AmazonS3URI uri = getAmazonS3URI(s3BucketConfiguration.getName(), tableName, null);
        final AmazonS3 s3Client = blobPlacement.getS3Client();

        return listObjects(s3Client, uri, fromBlobIdExclusive, s3BucketConfiguration.getScanBatchSize(), limit);
    }

    private Stream<S3ObjectSummary> listObjects(final AmazonS3 s3Client, final AmazonS3URI uri, @Nullable final String fromBlobIdExclusive, int scanBatchSize, final long limit) {
        Stream<S3ObjectSummary> summaryStream = Stream.<S3ObjectSummary>builder().build();

        final ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(uri.getBucket())
                .withPrefix(uri.getKey())
                .withStartAfter(fromBlobIdExclusive)
                .withMaxKeys(Math.toIntExact(Math.min(limit, scanBatchSize)));
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
                .limit(limit);
    }

    public void delete(final String tableName, final String tablePlacement) {
        final AtomicInteger counter = new AtomicInteger();
        BlobPlacement blobPlacement = _placementCache.getUnchecked(tablePlacement);
        S3BucketConfiguration s3BucketConfiguration = blobPlacement.getS3BucketConfiguration();
        final AmazonS3URI uri = getAmazonS3URI(s3BucketConfiguration.getName(), tableName, null);
        LOGGER.debug("Delete table uri: {}", uri);

        final AmazonS3 writeS3Client = blobPlacement.getS3Client();
        list(tableName, tablePlacement, null, Long.MAX_VALUE)
                .map(s3ObjectSummary -> new AmazonS3URI(String.format("s3://%s/%s", uri.getBucket(), s3ObjectSummary.getKey())))
                .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / s3BucketConfiguration.getScanBatchSize()))
                .forEach((integer, uris) -> delete(writeS3Client, uris));
        delete(writeS3Client, Arrays.asList(uri));
    }

    public void delete(final String tableName, final String tablePlacement, final String blobId) {
        BlobPlacement blobPlacement = _placementCache.getUnchecked(tablePlacement);
        final AmazonS3URI uri = getAmazonS3URI(blobPlacement.getS3BucketConfiguration().getName(), tableName, blobId);
        final AmazonS3 writeS3Client = blobPlacement.getS3Client();
        delete(writeS3Client, Arrays.asList(uri));
    }

    private void delete(final AmazonS3 s3Client, final List<AmazonS3URI> uris) {
        //TODO consider to use Futures
        LOGGER.debug("Delete object uris: {}", uris);
        uris.forEach(uri -> delete(s3Client, uri));
    }

    private void delete(final AmazonS3 s3Client, final AmazonS3URI uri) {
        try {
            LOGGER.debug("Delete object uri {}", uri);
            s3Client.deleteObject(new DeleteObjectRequest(uri.getBucket(), uri.getKey()));

            final DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(uri.getBucket())
                    .withKeys(uri.getKey());
            final DeleteObjectsResult deleteObjectsResult = s3Client.deleteObjects(deleteObjectsRequest);

            final List<DeleteObjectsRequest.KeyVersion> keyVersionsMarkers = deleteObjectsResult.getDeletedObjects().stream()
                    .map(deletedObject -> new DeleteObjectsRequest.KeyVersion(deletedObject.getKey(), deletedObject.getDeleteMarkerVersionId()))
                    .collect(Collectors.toList());
            s3Client.deleteObjects(new DeleteObjectsRequest(uri.getBucket())
                    .withKeys(keyVersionsMarkers));
        } catch (final AmazonS3Exception e) {
            LOGGER.error("Failed to delete blob: {}", uri);
            throw new RuntimeException(e);
        }
    }

    public long count(final String tableName, final String tablePlacement) {
        return list(tableName, tablePlacement, null, Long.MAX_VALUE)
                .count();
    }
}
