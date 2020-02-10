package com.bazaarvoice.emodb.blob.core;

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
import com.bazaarvoice.emodb.blob.db.MetadataProvider;
import com.bazaarvoice.emodb.blob.db.StorageProvider;
import com.bazaarvoice.emodb.blob.db.StorageSummary;
import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Throwables;
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
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Stores large binary objects like photos, videos.
 */
public class DefaultBlobStore implements BlobStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultBlobStore.class);

    private final TableDAO _tableDao;
    private final StorageProvider _storageProvider;
    private final MetadataProvider _metadataProvider;

    /**
     * This meter was created to provide visibility for inconsistency
     * when blob storage contains data but metadata storage doesn't.
     */
    private final Meter _metaDataNotPresentMeter;

    @Inject
    public DefaultBlobStore(TableDAO tableDao,
                            StorageProvider storageProvider,
                            MetadataProvider metadataProvider,
                            MetricRegistry metricRegistry) {
        _tableDao = checkNotNull(tableDao, "tableDao");
        _storageProvider = checkNotNull(storageProvider, "storageProvider");
        _metadataProvider = checkNotNull(metadataProvider, "metadataProvider");
        _metaDataNotPresentMeter = metricRegistry.meter(getMetricName("data-inconsistency"));
    }

    private static String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.blob", "blobstore", name);
    }

    @Override
    public Iterator<com.bazaarvoice.emodb.blob.api.Table> listTables(@Nullable String fromTableExclusive, long limit) {
        checkArgument(limit > 0, "Limit must be >0");
        LimitCounter remaining = new LimitCounter(limit);
        return getTablesStream(fromTableExclusive, remaining)
                .map(DefaultBlobStore::toDefaultTable)
                .iterator();
    }

    private Stream<Table> getTablesStream(@Nullable String fromTableExclusive, LimitCounter remaining) {
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(_tableDao.list(fromTableExclusive, remaining), Spliterator.ORDERED), false)
                .filter(table -> !table.isInternal())
                .limit(remaining.remaining());
    }

    private static com.bazaarvoice.emodb.blob.api.Table toDefaultTable(Table table) {
        //noinspection unchecked
        Map<String, String> attributes = (Map) table.getAttributes();
        return new DefaultTable(table.getName(), table.getOptions(), attributes, table.getAvailability());
    }

    @Override
    public void createTable(String table, TableOptions options, Map<String, String> attributes, Audit audit) throws TableExistsException {
        checkLegalTableName(table);
        checkNotNull(options, "options");
        checkNotNull(attributes, "attributes");
        checkMapOfStrings(attributes, "attributes");  // Defensive check that generic type restrictions aren't bypassed
        checkNotNull(audit, "audit");
        _tableDao.create(table, options, attributes, audit);
    }

    private static void checkMapOfStrings(Map<?, ?> map, String message) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            checkArgument(entry.getKey() instanceof String, message);
            checkArgument(entry.getValue() instanceof String, message);
        }
    }

    @Override
    public void dropTable(String table, Audit audit) throws UnknownTableException {
        checkLegalTableName(table);
        checkNotNull(audit, "audit");
        _tableDao.drop(table, audit);
    }

    @Override
    public void purgeTableUnsafe(String tableName, Audit audit) throws UnknownTableException {
        checkLegalTableName(tableName);
        Table table = _tableDao.get(tableName);
        purgeTableUnsafe(table, audit);
    }

    private void purgeTableUnsafe(Table table, Audit audit) {
        _tableDao.audit(table.getName(), "purge", audit);

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
    public Map<String, String> getTableAttributes(String table) throws UnknownTableException {
        checkLegalTableName(table);
        return getAttributes(_tableDao.get(table));
    }

    @Override
    public void setTableAttributes(String table, Map<String, String> attributes, Audit audit) throws UnknownTableException {
        checkLegalTableName(table);
        checkNotNull(attributes, "attributes");
        checkMapOfStrings(attributes, "attributes");  // Defensive check that generic type restrictions aren't bypassed
        checkNotNull(audit, "audit");
        _tableDao.setAttributes(table, attributes, audit);
    }

    @Override
    public TableOptions getTableOptions(String table) throws UnknownTableException {
        checkLegalTableName(table);
        return _tableDao.get(table).getOptions();
    }

    @Override
    public long getTableApproximateSize(String tableName) throws UnknownTableException {
        checkLegalTableName(tableName);
        Table table = _tableDao.get(tableName);
        return _metadataProvider.countMetadata(table);
    }

    @Override
    public BlobMetadata getMetadata(String tableName, String blobId) throws BlobNotFoundException {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);

        Table table = _tableDao.get(tableName);

        return newMetadata(table, blobId, _metadataProvider.readMetadata(table, blobId));
    }

    @Override
    public Iterator<BlobMetadata> scanMetadata(String tableName, @Nullable String fromBlobIdExclusive, long limit) {
        checkLegalTableName(tableName);
        checkArgument(fromBlobIdExclusive == null || Names.isLegalBlobId(fromBlobIdExclusive), "fromBlobIdExclusive");
        checkArgument(limit > 0, "Limit must be >0");

        final Table table = _tableDao.get(tableName);

        // Stream back results.  Don't hold them all in memory at once.
        LimitCounter remaining = new LimitCounter(limit);
        return remaining.limit(Iterators.transform(_metadataProvider.scanMetadata(table, fromBlobIdExclusive, remaining),
                entry -> newMetadata(table, entry.getKey(), entry.getValue())));
    }

    private static BlobMetadata newMetadata(Table table, String blobId, StorageSummary s) {
        if (s == null) {
            throw new BlobNotFoundException(blobId);
        }
        Map<String, String> attributes = Maps.newTreeMap();
        attributes.putAll(s.getAttributes());
        attributes.putAll(getAttributes(table));
        Date timestamp = new Date(s.getTimestamp() / 1000); // Convert from microseconds
        return new DefaultBlobMetadata(blobId, timestamp, s.getLength(), s.getMD5(), s.getSHA1(), attributes);
    }

    @Override
    public Blob get(String table, final String blobId) throws BlobNotFoundException {
        return get(table, blobId, null);
    }

    @Override
    public Blob get(String tableName, final String blobId, @Nullable RangeSpecification rangeSpec)
            throws BlobNotFoundException {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);

        final Table table = _tableDao.get(tableName);

        // Read the metadata for the blob.  This should verify that all chunks are present and available for reading.
        final StorageSummary summary = _metadataProvider.readMetadata(table, blobId);
        BlobMetadata metadata = newMetadata(table, blobId, summary);

        // Support returning a specific byte range within the blob.
        final Range range;
        if (rangeSpec != null) {
            range = rangeSpec.getRange(summary.getLength());
            // Satisfiable range requests must return at least one byte (per HTTP spec).
            checkArgument(range.getOffset() >= 0 && range.getLength() > 0 &&
                    range.getOffset() + range.getLength() <= summary.getLength(), "Invalid byte range: %s", rangeSpec);
        } else {
            // If no range is specified, return the entire entity.  This may return zero bytes.
            range = new Range(0, summary.getLength());
        }

        return new DefaultBlob(metadata, range, _storageProvider.getObjectStreamSupplier(table, blobId, summary, range));
    }

    @Override
    public void put(String tableName, String blobId, InputSupplier<? extends InputStream> in, Map<String, String> attributes) throws IOException {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);
        checkNotNull(in, "in");
        checkNotNull(attributes, "attributes");

        Table table = _tableDao.get(tableName);

        StorageSummary summary = _storageProvider.putObject(table, blobId, in.getInput(), attributes);

        try {
            _metadataProvider.writeMetadata(table, blobId, summary);
        } catch (Throwable t) {
            LOGGER.error("Failed to upload metadata for table: {}, blobId: {}, attempt to delete blob. Exception: {}", tableName, blobId, t.getMessage());

            try {
                _storageProvider.deleteObject(table, blobId);
            } catch (Exception e1) {
                LOGGER.error("Failed to delete blob for table: {}, blobId: {}. Inconsistency between blob and metadata storages. Exception: {}", tableName, blobId, e1.getMessage());
                _metaDataNotPresentMeter.mark();
            } finally {
                Throwables.propagate(t);
            }
        }
    }

    @Override
    public void delete(String tableName, String blobId) {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);

        Table table = _tableDao.get(tableName);

        StorageSummary storageSummary = _metadataProvider.readMetadata(table, blobId);

        delete(table, blobId, storageSummary);
    }

    private void delete(Table table, String blobId, StorageSummary storageSummary) {
        if (storageSummary == null) {
            LOGGER.error("Metadata isn't present for table: {}, blobId: {}", table.getName(), blobId);
            throw new BlobNotFoundException(blobId);
        }
        _metadataProvider.deleteMetadata(table, blobId);
        try {
            _storageProvider.deleteObject(table, blobId);
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

    @SuppressWarnings({"unchecked"})
    private static Map<String, String> getAttributes(Table table) {
        // Coerce Map<String, Object> to Map<String, String>
        return (Map) table.getAttributes();
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

}
