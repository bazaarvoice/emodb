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
import com.bazaarvoice.emodb.blob.api.StreamSupplier;
import com.bazaarvoice.emodb.blob.db.StorageProvider;
import com.bazaarvoice.emodb.blob.db.StorageSummary;
import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.google.inject.Inject;
import org.apache.commons.codec.binary.Hex;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * Stores large binary objects like photos, videos.
 */
public class DefaultBlobStore implements BlobStore {
    private final TableDAO _tableDao;
    private final StorageProvider _storageProvider;

    @Inject
    public DefaultBlobStore(TableDAO tableDao, StorageProvider storageProvider) {
        _tableDao = checkNotNull(tableDao, "tableDao");
        _storageProvider = checkNotNull(storageProvider, "storageProvider");
    }

    @Override
    public Iterator<com.bazaarvoice.emodb.blob.api.Table> listTables(@Nullable String fromTableExclusive, long limit) {
        checkArgument(limit > 0, "Limit must be >0");

        LimitCounter remaining = new LimitCounter(limit);
        final Iterator<Table> tableIter = _tableDao.list(fromTableExclusive, remaining);
        return remaining.limit(new AbstractIterator<com.bazaarvoice.emodb.blob.api.Table>() {
            @Override
            protected com.bazaarvoice.emodb.blob.api.Table computeNext() {
                while (tableIter.hasNext()) {
                    Table table = tableIter.next();
                    if (!table.isInternal()) {
                        return toDefaultTable(table);
                    }
                }
                return endOfData();
            }
        });
    }

    private com.bazaarvoice.emodb.blob.api.Table toDefaultTable(Table table) {
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

    private void checkMapOfStrings(Map<?, ?> map, String message) {
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
        _tableDao.audit(tableName, "purge", audit);
        _storageProvider.purge(table);
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
        return _storageProvider.count(table);
    }

    @Override
    public BlobMetadata getMetadata(String tableName, String blobId) throws BlobNotFoundException {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);

        Table table = _tableDao.get(tableName);

        return newMetadata(table, blobId, _storageProvider.readMetadata(table, blobId));
    }

    @Override
    public Iterator<BlobMetadata> scanMetadata(String tableName, @Nullable String fromBlobIdExclusive, long limit) {
        checkLegalTableName(tableName);
        checkArgument(fromBlobIdExclusive == null || Names.isLegalBlobId(fromBlobIdExclusive), "fromBlobIdExclusive");
        checkArgument(limit > 0, "Limit must be >0");

        final Table table = _tableDao.get(tableName);

        // Stream back results.  Don't hold them all in memory at once.
        LimitCounter remaining = new LimitCounter(limit);
        return remaining.limit(Iterators.transform(_storageProvider.scanMetadata(table, fromBlobIdExclusive, remaining),
                new Function<Map.Entry<String, StorageSummary>, BlobMetadata>() {
                    @Override
                    public BlobMetadata apply(Map.Entry<String, StorageSummary> entry) {
                        return newMetadata(table, entry.getKey(), entry.getValue());
                    }
                }));
    }

    private BlobMetadata newMetadata(Table table, String blobId, StorageSummary s) {
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
        final StorageSummary summary = _storageProvider.readMetadata(table, blobId);
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

        return new DefaultBlob(metadata, range, new StreamSupplier() {
            @Override
            public void writeTo(OutputStream out) throws IOException {
                readChunks(table, blobId, range, summary, out);
            }
        });
    }

    private void readChunks(Table table, String blobId, Range range, StorageSummary summary, OutputStream out)
            throws IOException {
        if (range.getLength() == 0) {
            return; // Nothing to do
        }
        // Calculate the location of the first byte
        long start = range.getOffset();
        int startChunk = (int)(start / summary.getChunkSize());
        int startOffset = (int)(start % summary.getChunkSize());
        // Calculate the location of the last byte
        long end = range.getOffset() + range.getLength() - 1; // Inclusive
        int endChunk = (int)(end / summary.getChunkSize());
        int endLimit = (int)(end % summary.getChunkSize()) + 1; // Exclusive

        // A more aggressive solution like the Astyanax ObjectReader recipe would improve performance by issuing
        // the reads in parallel.
        for (int i = startChunk; i <= endChunk; i++) {
            ByteBuffer chunk = _storageProvider.readChunk(table, blobId, i, summary.getTimestamp());
            if (chunk == null) {
                throw new IOException(format("Blob chunk %d is missing: %s", i, blobId));
            }

            // Adjust the start and end of the byte buffer if fetching a range of bytes, not the entire blob.
            int position = chunk.position();
            if (i == startChunk) {
                chunk.position(position + startOffset);
            }
            if (i == endChunk) {
                chunk.limit(position + endLimit);
            }

            // Copy the chunk bytes to the output stream.
            copyTo(chunk, out);
        }
    }

    /**
     * Copy the contents of a ByteBuffer to an OutputStream.
     */
    private void copyTo(ByteBuffer buf, OutputStream out) throws IOException {
        if (!buf.hasRemaining()) {
            return;
        }
        if (buf.hasArray()) {
            // Fast copy if the buffer is backed by an array (which should be the case)
            out.write(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        } else {
            // Slow copy otherwise
            byte[] bytes = new byte[4096];
            do {
                buf.get(bytes, 0, Math.min(bytes.length, buf.remaining()));
                out.write(bytes);
            } while (buf.hasRemaining());
        }
    }

    @Override
    public void put(String tableName, String blobId, InputSupplier<? extends InputStream> in, Map<String,String> attributes, @Nullable Duration ttl) throws IOException {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);
        checkNotNull(in, "in");
        checkNotNull(attributes, "attributes");

        Table table = _tableDao.get(tableName);

        long timestamp = _storageProvider.getCurrentTimestamp(table);
        int chunkSize = _storageProvider.getDefaultChunkSize();

        DigestInputStream md5In = new DigestInputStream(in.getInput(), getMessageDigest("MD5"));
        DigestInputStream sha1In = new DigestInputStream(md5In, getMessageDigest("SHA-1"));

        // A more aggressive solution like the Astyanax ObjectWriter recipe would improve performance by pipelining
        // reading the input stream and writing chunks, and issuing the writes in parallel.
        byte[] bytes = new byte[chunkSize];
        long length = 0;
        int chunkCount = 0;
        for (;;) {
            int chunkLength;
            try {
                chunkLength = ByteStreams.read(sha1In, bytes, 0, bytes.length);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            if (chunkLength == 0) {
                break;
            }
            ByteBuffer buffer = ByteBuffer.wrap(bytes, 0, chunkLength);
            _storageProvider.writeChunk(table, blobId, chunkCount, buffer, ttl, timestamp);
            length += chunkLength;
            chunkCount++;
        }

        // Include two types of hash: md5 (because it's common) and sha1 (because it's secure)
        String md5 = Hex.encodeHexString(md5In.getMessageDigest().digest());
        String sha1 = Hex.encodeHexString(sha1In.getMessageDigest().digest());

        StorageSummary summary = new StorageSummary(length, chunkCount, chunkSize, md5, sha1, attributes, timestamp);
        _storageProvider.writeMetadata(table, blobId, summary, ttl);
    }

    @Override
    public void delete(String tableName, String blobId) {
        checkLegalTableName(tableName);
        checkLegalBlobId(blobId);

        Table table = _tableDao.get(tableName);

        _storageProvider.deleteObject(table, blobId, null);
    }

    @Override
    public Collection<String> getTablePlacements() {
        return _tableDao.getTablePlacements(false /*includeInternal*/, false /*localOnly*/);
    }

    @SuppressWarnings({"unchecked"})
    private Map<String, String> getAttributes(Table table) {
        // Coerce Map<String, Object> to Map<String, String>
        return (Map) table.getAttributes();
    }

    private static MessageDigest getMessageDigest(String algorithmName) {
        try {
            return MessageDigest.getInstance(algorithmName);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private void checkLegalTableName(String table) {
        checkArgument(Names.isLegalTableName(table),
                "Table name must be a lowercase ASCII string between 1 and 255 characters in length. " +
                        "Allowed punctuation characters are -.:@_ and the table name may not start with a single underscore character. " +
                        "An example of a valid table name would be 'photo:testcustomer'.");
    }

    private void checkLegalBlobId(String blobId) {
        checkArgument(Names.isLegalBlobId(blobId),
                "Blob IDs must be ASCII strings between 1 and 255 characters in length. " +
                        "Whitespace, ISO control characters and certain punctuation characters that aren't generally allowed in file names are excluded.");
    }
}
