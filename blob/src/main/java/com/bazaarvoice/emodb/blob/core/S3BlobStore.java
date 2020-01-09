package com.bazaarvoice.emodb.blob.core;

import com.amazonaws.services.s3.model.ObjectMetadata;
import com.bazaarvoice.emodb.blob.api.Blob;
import com.bazaarvoice.emodb.blob.api.BlobMetadata;
import com.bazaarvoice.emodb.blob.api.BlobNotFoundException;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.DefaultBlob;
import com.bazaarvoice.emodb.blob.api.DefaultBlobMetadata;
import com.bazaarvoice.emodb.blob.api.DefaultTable;
import com.bazaarvoice.emodb.blob.api.Names;
import com.bazaarvoice.emodb.blob.api.Range;
import com.bazaarvoice.emodb.blob.api.RangeSpecification;
import com.bazaarvoice.emodb.blob.api.Table;
import com.bazaarvoice.emodb.blob.db.MetadataProvider;
import com.bazaarvoice.emodb.blob.db.StorageSummary;
import com.bazaarvoice.emodb.blob.db.s3.S3StorageProvider;
import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.io.InputSupplier;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;

public class S3BlobStore implements BlobStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(S3BlobStore.class);

    private final TableDAO _tableDao;
    private final S3StorageProvider _s3StorageProvider;
    private final MetadataProvider _metadataProvider;

    /**
     * This meter was created to provide visibility for inconsistency
     * when blob storage contains data but metadata storage doesn't.
     */
    private final Meter _metaDataNotPresentMeter;

    @Inject
    public S3BlobStore(final TableDAO tableDao,
                       final S3StorageProvider s3StorageProvider,
                       final MetadataProvider metadataProvider,
                       final MetricRegistry metricRegistry) {
        _tableDao = Objects.requireNonNull(tableDao);
        _s3StorageProvider = Objects.requireNonNull(s3StorageProvider);
        _metadataProvider = Objects.requireNonNull(metadataProvider, "metadataProvider");
        _metaDataNotPresentMeter = metricRegistry.meter(getMetricName("data-inconsistency"));
    }

    private static String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.blob", "s3", name);
    }

    @Override
    public Iterator<Table> listTables(@Nullable final String fromTableExclusive, final long limit) {
        checkArgument(limit > 0, "Limit must be >0");

        LimitCounter remaining = new LimitCounter(limit);
        final Iterator<com.bazaarvoice.emodb.table.db.Table> tableIter = _tableDao.list(fromTableExclusive, remaining);

        return remaining.limit(new AbstractIterator<com.bazaarvoice.emodb.blob.api.Table>() {
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
    public void createTable(final String table, final TableOptions options, final Map<String, String> attributes, final Audit audit) {
        checkLegalTableName(table);
        Objects.requireNonNull(options, "options");
        Objects.requireNonNull(attributes, "attributes");
        checkMapOfStrings(attributes, "attributes");  // Defensive check that generic type restrictions aren't bypassed
        Objects.requireNonNull(audit, "audit");

        _tableDao.create(table, options, attributes, audit);
    }

    @Override
    public void dropTable(final String tableName, final Audit audit) {
        checkLegalTableName(tableName);
        Objects.requireNonNull(audit, "audit");

        _tableDao.drop(tableName, audit);
    }

    @Override
    public void purgeTableUnsafe(final String tableName, final Audit audit) {
        checkLegalTableName(tableName);

        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);
        _tableDao.audit(tableName, "purge", audit);

        AtomicLong failedCounter = new AtomicLong();
        AtomicLong totalCounter = new AtomicLong();

        _metadataProvider.scanMetadata(table, null, LimitCounter.max()).forEachRemaining(entry -> {
            try {
                delete(table, entry.getKey(), entry.getValue());
            } catch (Throwable t) {
                failedCounter.getAndAdd(1);
            } finally {
                totalCounter.getAndAdd(1);
            }
        });

//TODO consider to remove everything in S3
//        String tablePlacement = getTablePlacement(toDefaultTable(table));
//        _s3StorageProvider.deleteTable(tableName, tablePlacement);

        if (totalCounter.get() > 0) {
            if (failedCounter.get() > 0) {
                String message = String.format("Failed to purge %s of %s rows for table: %s.", failedCounter.get(), totalCounter.get(), table.getName());
                LOGGER.error(message);
                throw new RuntimeException(message);
            } else {
                LOGGER.info("Table: {} has been purged successfully, removed {} rows.", table.getName(), totalCounter.get());
            }
        } else {
            LOGGER.info("Attempting to purge table: {} that is already empty.", table.getName());
        }
    }

    @Override
    public boolean getTableExists(final String table) {
        checkLegalTableName(table);
        return _tableDao.exists(table);
    }

    @Override
    public boolean isTableAvailable(final String table) {
        checkLegalTableName(table);
        return _tableDao.get(table).getAvailability() != null;
    }

    @Override
    public com.bazaarvoice.emodb.blob.api.Table getTableMetadata(final String table) {
        checkLegalTableName(table);
        return toDefaultTable(_tableDao.get(table));
    }

    @Override
    public Map<String, String> getTableAttributes(final String table) {
        checkLegalTableName(table);
        return getAttributes(_tableDao.get(table));
    }

    @Override
    public void setTableAttributes(final String table, final Map<String, String> attributes, final Audit audit) {
        checkLegalTableName(table);
        Objects.requireNonNull(attributes, "attributes");
        checkMapOfStrings(attributes, "attributes");  // Defensive check that generic type restrictions aren't bypassed
        Objects.requireNonNull(audit, "audit");
        _tableDao.setAttributes(table, attributes, audit);
    }

    @Override
    public TableOptions getTableOptions(final String table) {
        checkLegalTableName(table);
        return getTableMetadata(table).getOptions();
    }

    @Override
    public long getTableApproximateSize(final String tableName) {
        checkLegalTableName(tableName);
        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);
        return _metadataProvider.countMetadata(table);
    }

    @Override
    public BlobMetadata getMetadata(final String tableName, final String blobId) {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);

        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);
        return createBlobMetadata(blobId, _metadataProvider.readMetadata(table, blobId), getAttributes(table));
    }

    @Override
    public Iterator<BlobMetadata> scanMetadata(final String tableName, @Nullable final String fromBlobIdExclusive, final long limit) {
        checkLegalTableName(tableName);
        checkArgument(fromBlobIdExclusive == null || Names.isLegalBlobId(fromBlobIdExclusive), "fromBlobIdExclusive");
        checkArgument(limit > 0, "Limit must be >0");

        final com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);

        // Stream back results.  Don't hold them all in memory at once.
        LimitCounter remaining = new LimitCounter(limit);
        Map<String, String> tableAttributes = getAttributes(table);

        return remaining.limit(Iterators.transform(_metadataProvider.scanMetadata(table, fromBlobIdExclusive, remaining),
                entry -> createBlobMetadata(entry.getKey(), entry.getValue(), tableAttributes)));
    }

    private static BlobMetadata createBlobMetadata(String blobId, StorageSummary s, Map<String, String> tableAttributes) {
        if (s == null) {
            throw new BlobNotFoundException(blobId);
        }
        Map<String, String> attributes = Maps.newTreeMap();
        attributes.putAll(s.getAttributes());
        attributes.putAll(tableAttributes);
        Date timestamp = new Date(s.getTimestamp() / 1000); // Convert from microseconds
        return new DefaultBlobMetadata(blobId, timestamp, s.getLength(), s.getMD5(), s.getSHA1(), attributes);
    }

    @Override
    public Blob get(final String table, final String blobId) {
        return get(table, blobId, null);
    }

    @Override
    public Blob get(final String tableName, final String blobId, @Nullable final RangeSpecification rangeSpec) {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);

        String tablePlacement = getTablePlacement(getTableMetadata(tableName));
        final BlobMetadata metadata = getMetadata(tableName, blobId);

        final Range range = getRange(rangeSpec, metadata.getLength());
        return new DefaultBlob(metadata, range, _s3StorageProvider.getObjectStreamSupplier(tableName, tablePlacement, blobId, range));
    }

    private static Range getRange(@Nullable RangeSpecification rangeSpec,
                                  long contentLength) {
        Range range;
        if (null != rangeSpec) {
            range = rangeSpec.getRange(contentLength);
            // Satisfiable range requests must return at least one byte (per HTTP spec).
            checkArgument(range.getOffset() >= 0 && range.getLength() > 0 &&
                    range.getOffset() + range.getLength() <= contentLength, "Invalid byte range: %s", rangeSpec);
        } else {
            // If no range is specified, return the entire entity.  This may return zero bytes.
            range = new Range(0, contentLength);
        }
        return range;
    }

    @Override
    public void put(final String tableName, final String blobId,
                    final InputSupplier<? extends InputStream> in,
                    final Map<String, String> attributes)
            throws IOException {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);
        Objects.requireNonNull(in, "in");
        Objects.requireNonNull(attributes, "attributes");

        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);
        String tablePlacement = getTablePlacement(getTableMetadata(tableName));
        ObjectMetadata om = _s3StorageProvider.putObject(tableName, tablePlacement, blobId, in.getInput(), attributes);
        try {
            StorageSummary storageSummary = createStorageSummary(blobId, om);
            _metadataProvider.writeMetadata(table, blobId, storageSummary);
        } catch (Throwable t) {
            LOGGER.error("Failed to upload metadata for table: {}, blobId: {}, attempt to delete blob. Exception: {}", tableName, blobId, t.getMessage());

            try {
                _s3StorageProvider.deleteObject(tableName, tablePlacement, blobId);
            } catch (Exception e1) {
                LOGGER.error("Failed to delete blob for table: {}, blobId: {}. Inconsistency between blob and metadata storages. Exception: {}", tableName, blobId, e1.getMessage());
                _metaDataNotPresentMeter.mark();
            } finally {
                Throwables.propagate(t);
            }
        }
    }

    private static StorageSummary createStorageSummary(final String blobId,
                                                   final ObjectMetadata om) {
        if (null == om) {
            throw new BlobNotFoundException(blobId);
        }

        Map<String, String> attributes = new HashMap<>();
        attributes.putAll(om.getUserMetadata());
        String md5 = attributes.remove("md5");
        String sha1 = attributes.remove("sha-1");

        long contentLength = om.getContentLength();
        attributes.put("contentType", om.getContentType());
        attributes.put("contentLength", String.valueOf(contentLength));

        return new StorageSummary(contentLength, 1, Math.toIntExact(contentLength), md5, sha1, attributes, om.getLastModified().getTime());
    }

    @Override
    public void delete(final String tableName, final String blobId) {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);
        com.bazaarvoice.emodb.table.db.Table table = _tableDao.get(tableName);

        StorageSummary storageSummary = _metadataProvider.readMetadata(table, blobId);
        delete(table, blobId, storageSummary);
    }

    private void delete(com.bazaarvoice.emodb.table.db.Table table, String blobId, StorageSummary storageSummary) {
        if (storageSummary == null) {
            LOGGER.error("Metadata isn't present for table: {}, blobId: {}", table.getName(), blobId);
            throw new BlobNotFoundException(blobId);
        }
        _metadataProvider.deleteMetadata(table, blobId);
        String tablePlacement = getTablePlacement(toDefaultTable(table));

        try {
            _s3StorageProvider.deleteObject(table.getName(), tablePlacement, blobId);
        } catch (Throwable t) {
            LOGGER.error("Failed to delete blob for table: {}, blobId: {}, attempt to revert metadata deletion. Exception: {}", table.getName(), blobId, t.getMessage());
            try {
                _metadataProvider.writeMetadata(table, blobId, storageSummary);
            } catch (Exception e1) {
                LOGGER.error("Failed to revert metadata deletion for table: {}, blobId: {}. Inconsistency between blob and metadata storages. Exception: {}", table.getName(), blobId, e1.getMessage());
                _metaDataNotPresentMeter.mark();
            } finally {
                Throwables.propagate(t);
            }
        }
    }

    @Override
    public Collection<String> getTablePlacements() {
        return _tableDao.getTablePlacements(false /*includeInternal*/, false /*localOnly*/);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> getAttributes(com.bazaarvoice.emodb.table.db.Table table) {
        // Coerce Map<String, Object> to Map<String, String>
        return (Map) table.getAttributes();
    }

    private static void checkMapOfStrings(final Map<?, ?> map, final String message) {
        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            checkArgument(entry.getKey() instanceof String, message);
            checkArgument(entry.getValue() instanceof String, message);
        }
    }

    private static void checkLegalTableName(final String table) {
        checkArgument(Names.isLegalTableName(table),
                "Table name must be a lowercase ASCII string between 1 and 255 characters in length. " +
                        "Allowed punctuation characters are -.:@_ and the table name may not start with a single underscore character. " +
                        "An example of a valid table name would be 'photo:testcustomer'.");
    }

    private static void checkLegalBlobId(final String blobId) {
        checkArgument(Names.isLegalBlobId(blobId),
                "Blob IDs must be ASCII strings between 1 and 255 characters in length. " +
                        "Whitespace, ISO control characters and certain punctuation characters that aren't generally allowed in file names are excluded.");
    }

    private static String getTablePlacement(Table table) {
        TableAvailability availability = table.getAvailability();
        if (null != availability) {
            return availability.getPlacement();
        }
        // If the table isn't available locally then defer to it's placement from the table options.
        // If the user doesn't have permission the permission check will fail.  If he does the permission
        // check won't fail but another more informative exception will likely be thrown.

        return table.getOptions().getPlacement();
    }
}
