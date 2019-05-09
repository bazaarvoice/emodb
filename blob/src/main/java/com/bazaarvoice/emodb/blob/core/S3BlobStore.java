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
import com.bazaarvoice.emodb.blob.db.s3.S3StorageProvider;
import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableAvailability;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.google.common.collect.AbstractIterator;
import com.google.common.io.InputSupplier;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;

public class S3BlobStore implements BlobStore {

    private final TableDAO _tableDao;
    private final S3StorageProvider _s3StorageProvider;

    @Inject
    public S3BlobStore(final TableDAO tableDao,
                       final S3StorageProvider s3StorageProvider) {
        _tableDao = Objects.requireNonNull(tableDao);
        _s3StorageProvider = Objects.requireNonNull(s3StorageProvider);
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
        String tablePlacement = getTablePlacement(getTableMetadata(tableName));

        _tableDao.audit(tableName, "purge", audit);
        _s3StorageProvider.delete(tableName, tablePlacement);
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
        String tablePlacement = getTablePlacement(getTableMetadata(tableName));

        return _s3StorageProvider.count(tableName, tablePlacement);
    }

    @Override
    public BlobMetadata getMetadata(final String tableName, final String blobId) {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);
        Table tableMetadata = getTableMetadata(tableName);
        String tablePlacement = getTablePlacement(tableMetadata);

        ObjectMetadata objectMetadata = _s3StorageProvider.getObjectMetadata(tableName, tablePlacement, blobId);
        return createBlobMetadata(blobId, objectMetadata, tableMetadata.getAttributes());
    }

    @Override
    public Iterator<BlobMetadata> scanMetadata(final String tableName, @Nullable final String fromBlobIdExclusive, final long limit) {
        checkLegalTableName(tableName);

        Table tableMetadata = getTableMetadata(tableName);
        String tablePlacement = getTablePlacement(tableMetadata);
        Map<String, String> attributes = tableMetadata.getAttributes();

        return _s3StorageProvider.list(tableName, tablePlacement, fromBlobIdExclusive, limit)
                .map(summary -> {
                    String blobId = summary.getKey().substring(summary.getKey().lastIndexOf('/') + 1);
                    return createBlobMetadata(blobId, _s3StorageProvider.getObjectMetadata(tableName, tablePlacement, blobId), attributes);
                })
                .iterator();
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

    private static BlobMetadata createBlobMetadata(final String blobId,
                                                   final ObjectMetadata om,
                                                   final Map<String, String> tableAttributes) {
        if (null == om) {
            throw new BlobNotFoundException(blobId);
        }

        Map<String, String> attributes = new HashMap<>();
        attributes.putAll(om.getUserMetadata());
        attributes.putAll(tableAttributes);
        attributes.put("contentType", om.getContentType());
        attributes.put("contentLength", String.valueOf(om.getContentLength()));

        return new DefaultBlobMetadata(blobId, om.getLastModified(), om.getContentLength(), om.getUserMetaDataOf("MD5"), om.getUserMetaDataOf("SHA-1"), attributes);
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
        String tablePlacement = getTablePlacement(getTableMetadata(tableName));

        _s3StorageProvider.putObject(tableName, tablePlacement, blobId, in.getInput(), attributes);
    }

    @Override
    public void delete(final String tableName, final String blobId) {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);
        String tablePlacement = getTablePlacement(getTableMetadata(tableName));

        _s3StorageProvider.delete(tableName, tablePlacement, blobId);
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
