package com.bazaarvoice.emodb.blob.core;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.bazaarvoice.emodb.blob.api.Blob;
import com.bazaarvoice.emodb.blob.api.BlobMetadata;
import com.bazaarvoice.emodb.blob.api.BlobNotFoundException;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.DefaultBlob;
import com.bazaarvoice.emodb.blob.api.DefaultBlobMetadata;
import com.bazaarvoice.emodb.blob.api.DefaultTable;
import com.bazaarvoice.emodb.blob.api.Names;
import com.bazaarvoice.emodb.blob.api.Range;
import com.bazaarvoice.emodb.blob.api.RangeNotSatisfiableException;
import com.bazaarvoice.emodb.blob.api.RangeSpecification;
import com.bazaarvoice.emodb.blob.api.Table;
import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementsUnderMove;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.io.InputSupplier;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static com.google.common.base.Preconditions.checkArgument;

public class S3BlobStore implements BlobStore {

    private final TableDAO _tableDao;
    private final AmazonS3 _s3;
    private final Map<String, String> _placementToBucket;
    private final MetricRegistry _metricRegistry;
    private final Meter _blobReadMeter;
    private final Meter _blobWriteMeter;
    private final Timer _scanBatchTimer;
    private final Meter _scanReadMeter;

    @Inject
    public S3BlobStore(TableDAO tableDAO,
                       AmazonS3 s3,
                       @PlacementsUnderMove Map<String, String> placementToBucket,
                       MetricRegistry metricRegistry) {
        _tableDao = Objects.requireNonNull(tableDAO);
        _s3 = Objects.requireNonNull(s3);
        _placementToBucket = Objects.requireNonNull(placementToBucket);
        _metricRegistry = Objects.requireNonNull(metricRegistry);
        _blobReadMeter = metricRegistry.meter(getMetricName("blob-read"));
        _blobWriteMeter = metricRegistry.meter(getMetricName("blob-write"));
        _scanBatchTimer = metricRegistry.timer(getMetricName("scanBatch"));
        _scanReadMeter = metricRegistry.meter(getMetricName("scan-reads"));
    }

    private static String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.blob", "s3", name);
    }

    @Override
    public Iterator<Table> listTables(@Nullable String fromTableExclusive, long limit) {
        checkArgument(limit > 0, "Limit must be >0");

        LimitCounter remaining = new LimitCounter(limit);
        final Iterator<com.bazaarvoice.emodb.table.db.Table> tableIter = _tableDao.list(fromTableExclusive, remaining);
        return remaining.limit(new AbstractIterator<Table>() {
            @Override
            protected com.bazaarvoice.emodb.blob.api.Table computeNext() {
                while (tableIter.hasNext()) {
                    com.bazaarvoice.emodb.table.db.Table table = tableIter.next();
                    if (!table.isInternal()) {
                        return toDefaultTable(table);
                    }
                }
                return endOfData();
            }
        });
    }

    private static com.bazaarvoice.emodb.blob.api.Table toDefaultTable(com.bazaarvoice.emodb.table.db.Table table) {
        //noinspection unchecked
        Map<String, String> attributes = (Map) table.getAttributes();
        return new DefaultTable(table.getName(), table.getOptions(), attributes, table.getAvailability());
    }

    @Override
    public void createTable(String table, TableOptions options, Map<String, String> attributes, Audit audit)
            throws TableExistsException {
        checkLegalTableName(table);
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(attributes, "attributes");
        checkMapOfStrings(attributes, "attributes");  // Defensive check that generic type restrictions aren't bypassed
        Objects.requireNonNull(audit, "audit");
        _tableDao.create(table, options, attributes, audit);
    }

    @Override
    public void dropTable(String table, Audit audit)
            throws UnknownTableException {
        checkLegalTableName(table);
        Objects.requireNonNull(audit, "audit");
        _tableDao.drop(table, audit);
    }

    @Override
    public void purgeTableUnsafe(String tableName, Audit audit)
            throws UnknownTableException {
        checkLegalTableName(tableName);
        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);
        _tableDao.audit(tableName, "purge", audit);
        final int partitionSize = 1_000;
        Iterator<BlobMetadata> blobMetadataIterator = scanMetadata(table, null, Long.MAX_VALUE);
        Iterator<DeleteObjectsRequest.KeyVersion> keyVersionIterator = Iterators.transform(blobMetadataIterator, input -> new DeleteObjectsRequest.KeyVersion(getS3Key(table, input.getId())));
        UnmodifiableIterator<List<DeleteObjectsRequest.KeyVersion>> keyVersionIteratorsPartitioned = Iterators.partition(keyVersionIterator, partitionSize);
        keyVersionIteratorsPartitioned.forEachRemaining(keys -> _s3.deleteObjects(new DeleteObjectsRequest(getBucket(table)).withKeys(keys)));
    }

    private String getBucket(final com.bazaarvoice.emodb.table.db.Table table) {
        return _placementToBucket.get(getPlacement(table));
    }

    @Override
    public boolean getTableExists(String table) {
        checkLegalTableName(table);
        return _tableDao.exists(table);
    }

    @Override
    public boolean isTableAvailable(String table) {
        checkLegalTableName(table);
        return _tableDao.get(table).getAvailability() != null;
    }

    @Override
    public com.bazaarvoice.emodb.blob.api.Table getTableMetadata(String table) {
        checkLegalTableName(table);
        return toDefaultTable(_tableDao.get(table));
    }

    @Override
    public Map<String, String> getTableAttributes(String table)
            throws UnknownTableException {
        checkLegalTableName(table);
        return getAttributes(_tableDao.get(table));
    }

    @Override
    public void setTableAttributes(String table, Map<String, String> attributes, Audit audit)
            throws UnknownTableException {
        checkLegalTableName(table);
        Objects.requireNonNull(attributes, "attributes");
        checkMapOfStrings(attributes, "attributes");  // Defensive check that generic type restrictions aren't bypassed
        Objects.requireNonNull(audit, "audit");
        _tableDao.setAttributes(table, attributes, audit);
    }

    @Override
    public TableOptions getTableOptions(String table)
            throws UnknownTableException {
        checkLegalTableName(table);
        return _tableDao.get(table).getOptions();
    }

    @Override
    public long getTableApproximateSize(String tableName)
            throws UnknownTableException {
        checkLegalTableName(tableName);
        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);
        Iterator<BlobMetadata> blobMetadataIterator = scanMetadata(table, null, null);
        return Iterators.size(blobMetadataIterator);
    }

    @Override
    public BlobMetadata getMetadata(String tableName, String blobId)
            throws BlobNotFoundException {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);
        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);

        ObjectMetadata metadata;

        try {
            metadata = _s3.getObjectMetadata(getBucket(table), getS3Key(table, blobId));
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == Response.Status.NOT_FOUND.getStatusCode()) {
                metadata = null;
            } else {
                throw Throwables.propagate(e);
            }
        }

        return newMetadata(table, blobId, metadata);
    }

    private static String getS3TablePrefix(final com.bazaarvoice.emodb.table.db.Table table) {
        //TODO add prefix and placement
        String placement = getPlacement(table);
        return UriBuilder.fromPath("{arg1}").path("{arg2}").build(table.getName()).toASCIIString();
    }

    private static String getS3Key(final com.bazaarvoice.emodb.table.db.Table table, final String blobId) {
        return getS3TablePrefix(table) + blobId;
    }

    private Iterator<BlobMetadata> scanMetadata(com.bazaarvoice.emodb.table.db.Table table, @Nullable String fromBlobIdExclusive, long limit) {
        Iterator<S3ObjectSummary> summaryIterator = new AbstractIterator<S3ObjectSummary>() {
            int index;
            List<S3ObjectSummary> localSummaries;
            long remaining = limit;
            String continuationToken;

            @Override
            protected S3ObjectSummary computeNext() {

                if (remaining == 0) {
                    return endOfData();
                }

                if (localSummaries == null || index == localSummaries.size()) {
                    //TODO
                    ListObjectsV2Request request = new ListObjectsV2Request()
                            .withBucketName(getBucket(table))
                            .withPrefix(table.getName())
                            .withStartAfter(fromBlobIdExclusive)
                            .withMaxKeys(Math.toIntExact(Math.min(remaining, 1000)));
                    if (continuationToken != null) {
                        request.setContinuationToken(continuationToken);
                    }

                    ListObjectsV2Result result = _s3.listObjectsV2(request);
                    localSummaries = result.getObjectSummaries();
                    continuationToken = result.getNextContinuationToken();
                    if (continuationToken == null) {
                        remaining = localSummaries.size();
                    }

                    index = 0;
                }

                remaining--;
                return index < localSummaries.size() ? localSummaries.get(index++) : endOfData();
            }
        };

        return new MetadataIterator(summaryIterator, table);
    }

    @Override
    public Iterator<BlobMetadata> scanMetadata(String tableName, @Nullable String fromBlobIdExclusive, long limit) {
        checkLegalTableName(tableName);
        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);

        return scanMetadata(table, fromBlobIdExclusive, limit);
    }

    private class MetadataIterator extends AbstractIterator<BlobMetadata> {
        private final Iterator<S3ObjectSummary> _summaryIterator;
        private final com.bazaarvoice.emodb.table.db.Table _table;

        public MetadataIterator(Iterator<S3ObjectSummary> summaryIterator, com.bazaarvoice.emodb.table.db.Table table) {
            _summaryIterator = Objects.requireNonNull(summaryIterator);
            _table = Objects.requireNonNull(table);
        }

        @Override
        protected BlobMetadata computeNext() {
            if (_summaryIterator.hasNext()) {
                S3ObjectSummary summary = _summaryIterator.next();
                return newMetadata(_table, summary.getKey().substring(summary.getKey().lastIndexOf('/') + 1), _s3.getObjectMetadata(summary.getBucketName(), summary.getKey()));
            }
            return endOfData();
        }
    }

    @Override
    public Blob get(String table, String blobId) throws BlobNotFoundException {
        return get(table, blobId, null);
    }

    @Override
    public Blob get(String tableName, String blobId, @Nullable RangeSpecification rangeSpec)
            throws BlobNotFoundException, RangeNotSatisfiableException {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);

        final com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);

        BlobMetadata metadata = getMetadata(tableName, blobId);

        final Range range;
        if (null != rangeSpec) {
            range = rangeSpec.getRange(metadata.getLength());
            // Satisfiable range requests must return at least one byte (per HTTP spec).
            checkArgument(range.getOffset() >= 0 && range.getLength() > 0 &&
                    range.getOffset() + range.getLength() <= metadata.getLength(), "Invalid byte range: %s", rangeSpec);
        } else {
            // If no range is specified, return the entire entity.  This may return zero bytes.
            range = new Range(0, metadata.getLength());
        }

        GetObjectRequest rangeObjectRequest = new GetObjectRequest(getBucket(table), getS3Key(table, blobId))
                .withRange(range.getOffset(), range.getOffset() + range.getLength());
        S3Object object = _s3.getObject(rangeObjectRequest);
        return new DefaultBlob(metadata, range, out -> _blobReadMeter.mark(IOUtils.copy(object.getObjectContent(), out)));
    }

    private static BlobMetadata newMetadata(com.bazaarvoice.emodb.table.db.Table table, String blobId, ObjectMetadata objectMetadata) {
        if (objectMetadata == null) {
            throw new BlobNotFoundException(blobId);
        }
        Map<String, String> attributes = new TreeMap<>();
        attributes.putAll(objectMetadata.getUserMetadata());
        attributes.putAll(getAttributes(table));
        Date timestamp = objectMetadata.getLastModified(); // TODO Convert from microseconds
        return new DefaultBlobMetadata(blobId, timestamp, objectMetadata.getContentLength(), objectMetadata.getContentMD5(), objectMetadata.getETag(), attributes);
    }

    @Override
    public void put(String tableName, String blobId, final InputSupplier<? extends InputStream> in, Map<String, String> attributes, @Nullable Duration ttl)
            throws IOException {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);
        Objects.requireNonNull(in, "in");
        Objects.requireNonNull(attributes, "attributes");

        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);

        long timestamp = System.currentTimeMillis();

        // TODO: using the s3 api that we are currently using, we are unable to set the md5 hash on the objectMetadata like we need to
        DigestInputStream md5In = new DigestInputStream(in.getInput(), getMessageDigest("MD5"));
        DigestInputStream sha1In = new DigestInputStream(md5In, getMessageDigest("SHA-1"));

        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentMD5(md5In.toString());
        attributes.put("SHA-1", sha1In.toString());
        objectMetadata.setUserMetadata(attributes);
        //TODO
//        objectMetadata.setExpirationTimeRuleId("");
        _s3.putObject(getBucket(table), getS3Key(table, blobId), in.getInput(), objectMetadata);
        //TODO
//        Date expirationDate = new Date();
//        _s3.setBucketLifecycleConfiguration(new SetBucketLifecycleConfigurationRequest(getBucket(table), new BucketLifecycleConfiguration().withRules(new BucketLifecycleConfiguration.Rule().withExpirationDate(expirationDate))));
    }

    @Override
    public void delete(String tableName, String blobId) {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);

        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);
        _s3.deleteObject(getBucket(table), getS3Key(table, blobId));
    }

    @Override
    public Collection<String> getTablePlacements() {
        return _tableDao.getTablePlacements(false /*includeInternal*/, false /*localOnly*/);
    }

    private static Map<String, String> getAttributes(com.bazaarvoice.emodb.table.db.Table table) {
        // Coerce Map<String, Object> to Map<String, String>
        return (Map) table.getAttributes();
    }

    private static void checkMapOfStrings(Map<?, ?> map, String message) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            checkArgument(entry.getKey() instanceof String, message);
            checkArgument(entry.getValue() instanceof String, message);
        }
    }

    private static MessageDigest getMessageDigest(String algorithmName) {
        try {
            return MessageDigest.getInstance(algorithmName);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void checkLegalTableName(String table) {
        checkArgument(Names.isLegalTableName(table),
                "Table name must be a lowercase ASCII string between 1 and 255 characters in length. " +
                        "Allowed punctuation characters are -.:@_ and the table name may not start with a single underscore character. " +
                        "An example of a valid table name would be 'photo:testcustomer'.");
    }

    private static void checkLegalBlobId(String blobId) {
        checkArgument(Names.isLegalBlobId(blobId),
                "Blob IDs must be ASCII strings between 1 and 255 characters in length. " +
                        "Whitespace, ISO control characters and certain punctuation characters that aren't generally allowed in file names are excluded.");
    }

    private static String getPlacement(com.bazaarvoice.emodb.table.db.Table table) {
        TableAvailability availability = table.getAvailability();
        if (availability != null) {
            return availability.getPlacement();
        }
        // If the table isn't available locally then defer to it's placement from the table options.
        // If the user doesn't have permission the permission check will fail.  If he does the permission
        // check won't fail but another more informative exception will likely be thrown.

        return table.getOptions().getPlacement();
    }
}
