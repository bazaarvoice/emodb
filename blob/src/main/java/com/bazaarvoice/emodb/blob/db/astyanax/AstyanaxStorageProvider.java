package com.bazaarvoice.emodb.blob.db.astyanax;

import com.bazaarvoice.emodb.blob.BlobReadConsistency;
import com.bazaarvoice.emodb.blob.api.Names;
import com.bazaarvoice.emodb.blob.core.BlobPlacement;
import com.bazaarvoice.emodb.blob.db.MetadataProvider;
import com.bazaarvoice.emodb.blob.db.StorageProvider;
import com.bazaarvoice.emodb.blob.db.StorageSummary;
import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.cassandra.nio.BufferUtils;
import com.bazaarvoice.emodb.common.dropwizard.metrics.ParameterizedTimed;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxTable;
import com.bazaarvoice.emodb.table.db.astyanax.DataCopyDAO;
import com.bazaarvoice.emodb.table.db.astyanax.DataPurgeDAO;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnQuery;
import com.netflix.astyanax.serializers.AsciiSerializer;
import com.netflix.astyanax.serializers.CompositeSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Token;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.bazaarvoice.emodb.blob.core.Constants.S3_STORAGE_ATTRIBUTE_VALUE;
import static com.bazaarvoice.emodb.blob.core.Constants.STORAGE_ATTRIBUTE_NAME;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * A single row Cassandra implementation of {@link com.bazaarvoice.emodb.blob.db.StorageProvider}.
 * <p/>
 * Storing blobs in a single row means blobs can't be bigger than what a single Cassandra server can handle.  This is
 * unlikely to be a problem as long as it's not used to store large videos.  It also means that reads of a single large
 * blob can't be distributed across all servers in the ring but instead just N servers, where N is the replication
 * factor.
 * <p/>
 * The storage strategy is to chunk up the blob into smallish columns where each chunk can fit easily in memory.  To do
 * this, there it uses 3 classes of columns:
 * - A single metadata column with a {@link StorageSummary} object encoded as JSON.
 * - N "presence" columns, one per chunk.  These have no data, but are written at the same time as each chunk.  A
 * reader can quickly read the presence columns and verify that, if all presence columns exist, all the chunk columns
 * must also exist and be available (unless a concurrent writer re-writes the blob before the chunks can be retrieved).
 * - N chunk columns, sized so that each chunk can be transferred in a single thrift call without consuming too much
 * memory.
 * <p/>
 * The Astyanax com.netflix.astyanax.recipes.storage.CassandraChunkedStorageProvider implementation has a few
 * deficiencies that this avoids:
 * - The Astyanax v1.0.1 ChunkedStorage recipe has minor bugs: it ignores its
 * configured ConsistencyLevel, it doesn't have a way to set some ObjectMetadata attributes.
 * - The consistency story seems weak.  There's no way to test whether all chunks are available
 * before starting to stream back the results, and as a result the algorithm relies heavily
 * on retry to wait for replication.  If the retry algorithm waits too long (seconds) it
 * impacts the client, and if too many threads get stuck in retry loops it will DOS the
 * BlobStore service.
 * - If two different blobs are stored using the same IDs, we may end up with orphaned chunks
 * that never get cleaned up.  We'd have to write a M/R job that looks for them.
 * - If a blob is overwritten with new data, there is a period of time when readers could see
 * mixed results where part of the returned data is from the old blob and part is from the
 * new blob.
 * <p/>
 * On the other hand, the Astyanax recipes com.netflix.astyanax.recipes.storage.ObjectReader and
 * com.netflix.astyanax.recipes.storage.ObjectWriter are much more aggressive about retries and concurrency, so they
 * likely have better performance than this does.
 */
public class AstyanaxStorageProvider implements StorageProvider, MetadataProvider, DataCopyDAO, DataPurgeDAO {

    private enum ColumnGroup {
        A,  // Metadata encoded as JSON
        B,  // Chunk presence markers
        Z,  // Chunk bytes
    }

    private static final int DEFAULT_CHUNK_SIZE = 0x10000; // 64kb
    private static final ConsistencyLevel CONSISTENCY_STRONG = ConsistencyLevel.CL_LOCAL_QUORUM;
    private static final int MAX_SCAN_METADATA_BATCH_SIZE = 250;

    private final ConsistencyLevel _readConsistency;
    private final Token.TokenFactory _tokenFactory;
    private final Meter _blobReadMeter;
    private final Meter _blobMetadataReadMeter;
    private final Meter _blobWriteMeter;
    private final Meter _blobMetadataWriteMeter;
    private final Meter _blobDeleteMeter;
    private final Meter _blobMetadataDeleteMeter;
    private final Meter _blobCopyMeter;
    private final Meter _blobMetadataCopyMeter;
    private final Timer _scanBatchTimer;
    private final Meter _scanReadMeter;

    @Inject
    public AstyanaxStorageProvider(@BlobReadConsistency ConsistencyLevel readConsistency, MetricRegistry metricRegistry) {
        _readConsistency = Objects.requireNonNull(readConsistency, "readConsistency");
        _tokenFactory = new ByteOrderedPartitioner().getTokenFactory();
        _blobReadMeter = metricRegistry.meter(getMetricName("blob-read"));
        _blobWriteMeter = metricRegistry.meter(getMetricName("blob-write"));
        _blobDeleteMeter = metricRegistry.meter(getMetricName("blob-delete"));
        _blobCopyMeter = metricRegistry.meter(getMetricName("blob-copy"));
        _blobMetadataReadMeter = metricRegistry.meter(getMetricName("blob-metadata-read"));
        _blobMetadataWriteMeter = metricRegistry.meter(getMetricName("blob-metadata-write"));
        _blobMetadataDeleteMeter = metricRegistry.meter(getMetricName("blob-metadata-delete"));
        _blobMetadataCopyMeter = metricRegistry.meter(getMetricName("blob-metadata-copy"));
        _scanBatchTimer = metricRegistry.timer(getMetricName("scan-batch"));
        _scanReadMeter = metricRegistry.meter(getMetricName("scan-reads"));
    }

    private static String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.blob", "AstyanaxStorageProvider", name);
    }

    @Override
    public long getCurrentTimestamp(Table tbl) {
        AstyanaxTable table = (AstyanaxTable) Objects.requireNonNull(tbl, "table");
        AstyanaxStorage storage = table.getReadStorage();
        CassandraKeyspace keyspace = storage.getPlacement().getKeyspace();

        return keyspace.getAstyanaxKeyspace().getConfig().getClock().getCurrentTime();
    }

    @ParameterizedTimed(type = "AstyanaxStorageProvider")
    @Override
    public void writeChunk(Table tbl, String blobId, int chunkId, ByteBuffer data, long timestamp) {
        AstyanaxTable table = (AstyanaxTable) Objects.requireNonNull(tbl, "table");
        for (AstyanaxStorage storage : table.getWriteStorage()) {
            BlobPlacement placement = (BlobPlacement) storage.getPlacement();

            // Write two columns: one small one and one big one with the binary data.  Readers can query on
            // the presence of the small one to be confident that the big column has replicated and is available.
            MutationBatch mutation = placement.getKeyspace().prepareMutationBatch(CONSISTENCY_STRONG)
                    .setTimestamp(timestamp);
            mutation.withRow(placement.getBlobColumnFamily(), storage.getRowKey(blobId))
                    .putEmptyColumn(getColumn(ColumnGroup.B, chunkId))
                    .putColumn(getColumn(ColumnGroup.Z, chunkId), data);
            execute(mutation);

            _blobWriteMeter.mark(data.remaining());
        }
    }

    @ParameterizedTimed(type = "AstyanaxStorageProvider")
    @Override
    public ByteBuffer readChunk(Table tbl, String blobId, int chunkId, long timestamp) {
        AstyanaxTable table = (AstyanaxTable) Objects.requireNonNull(tbl, "table");
        AstyanaxStorage storage = table.getReadStorage();
        BlobPlacement placement = (BlobPlacement) storage.getPlacement();
        CassandraKeyspace keyspace = placement.getKeyspace();

        ColumnQuery<Composite> query = keyspace.prepareQuery(placement.getBlobColumnFamily(), _readConsistency)
                .getKey(storage.getRowKey(blobId))
                .getColumn(getColumn(ColumnGroup.Z, chunkId));
        OperationResult<Column<Composite>> operationResult;
        try {
            operationResult = query.execute();
        } catch (NotFoundException e) {
            return null;
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }
        Column<Composite> column = operationResult.getResult();
        if (column.getTimestamp() != timestamp) {
            return null;
        }
        ByteBuffer data = column.getByteBufferValue();

        _blobReadMeter.mark(data.remaining());
        return data;
    }

    @ParameterizedTimed(type = "AstyanaxStorageProvider")
    @Override
    public void deleteObject(Table tbl, String blobId) {
        AstyanaxTable table = (AstyanaxTable) Objects.requireNonNull(tbl, "table");
        for (AstyanaxStorage storage : table.getWriteStorage()) {
            BlobPlacement placement = (BlobPlacement) storage.getPlacement();

            // Do a column range query on all the B and Z columns.  Don't get the A columns with the metadata.
            Composite start = getColumnPrefix(ColumnGroup.B, Composite.ComponentEquality.LESS_THAN_EQUAL);
            Composite end = getColumnPrefix(ColumnGroup.Z, Composite.ComponentEquality.GREATER_THAN_EQUAL);
            ColumnList<Composite> columns = execute(placement.getKeyspace()
                    .prepareQuery(placement.getBlobColumnFamily(), _readConsistency)
                    .getKey(storage.getRowKey(blobId))
                    .withColumnRange(start, end, false, Integer.MAX_VALUE));

            deleteDataColumns(table, blobId, columns, CONSISTENCY_STRONG, null);
            _blobDeleteMeter.mark();
        }
    }

    @ParameterizedTimed(type = "AstyanaxStorageProvider")
    @Override
    public void writeMetadata(Table tbl, String blobId, StorageSummary summary) {
        AstyanaxTable table = (AstyanaxTable) Objects.requireNonNull(tbl, "table");
        for (AstyanaxStorage storage : table.getWriteStorage()) {
            BlobPlacement placement = (BlobPlacement) storage.getPlacement();

            MutationBatch mutation = placement.getKeyspace().prepareMutationBatch(CONSISTENCY_STRONG)
                    .setTimestamp(summary.getTimestamp());
            mutation.withRow(placement.getBlobColumnFamily(), storage.getRowKey(blobId))
                    .putColumn(getColumn(ColumnGroup.A, 0), JsonHelper.asJson(summary));
            execute(mutation);
        }
        _blobMetadataWriteMeter.mark();
    }

    @ParameterizedTimed(type = "AstyanaxStorageProvider")
    @Override
    public StorageSummary readMetadata(Table tbl, String blobId) {
        AstyanaxTable table = (AstyanaxTable) Objects.requireNonNull(tbl, "table");
        Objects.requireNonNull(blobId, "blobId");
        AstyanaxStorage storage = table.getReadStorage();
        BlobPlacement placement = (BlobPlacement) storage.getPlacement();

        // Do a column range query on all the A and B columns.  Don't get the Z columns with the binary data.
        Composite start = getColumnPrefix(ColumnGroup.A, Composite.ComponentEquality.LESS_THAN_EQUAL);
        Composite end = getColumnPrefix(ColumnGroup.B, Composite.ComponentEquality.GREATER_THAN_EQUAL);
        ColumnList<Composite> columns = execute(placement.getKeyspace()
                .prepareQuery(placement.getBlobColumnFamily(), _readConsistency)
                .getKey(storage.getRowKey(blobId))
                .withColumnRange(start, end, false, Integer.MAX_VALUE));

        boolean checkConsistency = !S3_STORAGE_ATTRIBUTE_VALUE.equals(tbl.getAttributes().get(STORAGE_ATTRIBUTE_NAME));

        StorageSummary summary = toStorageSummary(columns, checkConsistency);
        if (summary == null) {
            return null;
        }

//      TODO should be removed for blob s3 migration
//      Cleanup older versions of the blob, if any (unlikely).
        deleteDataColumns(table, blobId, columns, ConsistencyLevel.CL_ANY, summary.getTimestamp());

        _blobMetadataReadMeter.mark();
        return summary;
    }

    @ParameterizedTimed(type = "AstyanaxStorageProvider")
    @Override
    public void deleteMetadata(Table tbl, String blobId) {
        AstyanaxTable table = (AstyanaxTable) Objects.requireNonNull(tbl, "table");
        Objects.requireNonNull(blobId);

        for (AstyanaxStorage storage : table.getWriteStorage()) {
            BlobPlacement placement = (BlobPlacement) storage.getPlacement();
            MutationBatch mutation = placement.getKeyspace().prepareMutationBatch(CONSISTENCY_STRONG);
            mutation.withRow(placement.getBlobColumnFamily(), storage.getRowKey(blobId))
                    .deleteColumn(getColumn(ColumnGroup.A, 0));
            execute(mutation);
            _blobMetadataDeleteMeter.mark(mutation.getRowCount());
        }
    }

    @ParameterizedTimed(type = "AstyanaxStorageProvider")
    @Override
    public long countMetadata(Table tbl) {
        return countRowsInColumn(tbl, ColumnGroup.A);
    }

    private static StorageSummary toStorageSummary(ColumnList<Composite> columns, boolean checkDataConsistency) {
        if (columns.size() == 0) {
            return null;
        }

        // Read the summary column with the attributes, length etc.
        Column<Composite> summaryColumn = columns.getColumnByIndex(0);
        if (summaryColumn == null || !matches(summaryColumn.getName(), ColumnGroup.A, 0)) {
            return null;
        }
        StorageSummary summary = JsonHelper.fromJson(summaryColumn.getStringValue(), StorageSummary.class);

        if (checkDataConsistency) {
            // Check that all the chunks are available.  Some may still be in the process of being written or replicated.
            if (columns.size() < 1 + summary.getChunkCount()) {
                return null;
            }
            for (int chunkId = 0; chunkId < summary.getChunkCount(); chunkId++) {
                Column<Composite> presence = columns.getColumnByIndex(chunkId + 1);
                if (presence == null ||
                        !matches(presence.getName(), ColumnGroup.B, chunkId) ||
                        presence.getTimestamp() != summary.getTimestamp()) {
                    return null;
                }
            }
        }
        return summary;
    }

    @ParameterizedTimed(type = "AstyanaxStorageProvider")
    @Override
    public Iterator<Map.Entry<String, StorageSummary>> scanMetadata(Table tbl, @Nullable String fromBlobIdExclusive,
                                                                    final LimitCounter limit) {
        Objects.requireNonNull(tbl, "table");
        checkArgument(fromBlobIdExclusive == null || Names.isLegalBlobId(fromBlobIdExclusive), "fromBlobIdExclusive");
        checkArgument(limit.remaining() > 0, "Limit must be >0");

        final AstyanaxTable table = (AstyanaxTable) tbl;
        AstyanaxStorage storage = table.getReadStorage();
        final BlobPlacement placement = (BlobPlacement) storage.getPlacement();

        // Do a column range query on all the A and B columns.  Don't get the Z columns with the binary data.
        CompositeSerializer colSerializer = CompositeSerializer.get();
        final ByteBufferRange columnRange = new RangeBuilder()
                .setStart(getColumnPrefix(ColumnGroup.A, Composite.ComponentEquality.LESS_THAN_EQUAL), colSerializer)
                .setEnd(getColumnPrefix(ColumnGroup.B, Composite.ComponentEquality.GREATER_THAN_EQUAL), colSerializer)
                .build();

        // Loop over all the range prefixes (256 of them) and, for each, execute Cassandra queries to page through the
        // records with that prefix.
        final Iterator<ByteBufferRange> scanIter = storage.scanIterator(fromBlobIdExclusive);
        boolean checkConsistency = !S3_STORAGE_ATTRIBUTE_VALUE.equals(tbl.getAttributes().get(STORAGE_ATTRIBUTE_NAME));

        return touch(Iterators.concat(new AbstractIterator<Iterator<Map.Entry<String, StorageSummary>>>() {
            @Override
            protected Iterator<Map.Entry<String, StorageSummary>> computeNext() {
                if (scanIter.hasNext()) {
                    ByteBufferRange keyRange = scanIter.next();
                    return decodeMetadataRows(scanInternal(placement, keyRange, columnRange, limit), table, checkConsistency);
                }
                return endOfData();
            }
        }));
    }

    private static Iterator<Map.Entry<String, StorageSummary>> decodeMetadataRows(
            final Iterator<Row<ByteBuffer, Composite>> rowIter, final AstyanaxTable table, boolean checkConsistency) {
        return new AbstractIterator<Map.Entry<String, StorageSummary>>() {
            @Override
            protected Map.Entry<String, StorageSummary> computeNext() {
                while (rowIter.hasNext()) {
                    Row<ByteBuffer, Composite> row = rowIter.next();
                    ByteBuffer key = row.getKey();
                    ColumnList<Composite> columns = row.getColumns();

                    String blobId = AstyanaxStorage.getContentKey(key);

                    StorageSummary summary = toStorageSummary(columns, checkConsistency);
                    if (summary == null) {
                        continue;  // Partial blob, parts may still be replicating.
                    }

                    // TODO should be removed for blob s3 migration
                    // Cleanup older versions of the blob, if any (unlikely).
                    deleteDataColumns(table, blobId, columns, ConsistencyLevel.CL_ANY, summary.getTimestamp());

                    return Maps.immutableEntry(blobId, summary);
                }
                return endOfData();
            }
        };
    }

    private long countRowsInColumn(Table tbl, ColumnGroup column) {
        Objects.requireNonNull(tbl, "table");

        AstyanaxTable table = (AstyanaxTable) tbl;
        AstyanaxStorage storage = table.getReadStorage();
        BlobPlacement placement = (BlobPlacement) storage.getPlacement();

        // Limit the # of columns to retrieve since we just want to count rows, but we need one column to ignore range
        // ghosts.
        CompositeSerializer colSerializer = CompositeSerializer.get();
        ByteBufferRange columnRange = new RangeBuilder()
                .setStart(getColumnPrefix(column, Composite.ComponentEquality.LESS_THAN_EQUAL), colSerializer)
                .setEnd(getColumnPrefix(column, Composite.ComponentEquality.GREATER_THAN_EQUAL), colSerializer)
                .setLimit(1)
                .build();
        LimitCounter unlimited = LimitCounter.max();

        // Range query all the shards and count the number of rows in each.
        long count = 0;
        Iterator<ByteBufferRange> scanIter = storage.scanIterator(null);
        while (scanIter.hasNext()) {
            ByteBufferRange keyRange = scanIter.next();
            Iterator<Row<ByteBuffer, Composite>> rowIter = scanInternal(placement, keyRange, columnRange, unlimited);
            while (rowIter.hasNext()) {
                if (!rowIter.next().getColumns().isEmpty()) {
                    count++;
                }
            }
        }
        return count;
    }

    // DataCopyDAO
    @Override
    public void copy(AstyanaxStorage source, AstyanaxStorage dest, Runnable progress) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(dest, "dest");

        Iterator<ByteBufferRange> scanIter = source.scanIterator(null);
        while (scanIter.hasNext()) {
            ByteBufferRange range = scanIter.next();
//        metadata copy will be conditional for s3 migration
            copyRange(range, source, dest, true, true, progress);
        }
    }

    private void copyRange(ByteBufferRange keyRange, AstyanaxStorage source, AstyanaxStorage dest, boolean copyMetadata, boolean copyData, Runnable progress) {
        BlobPlacement sourcePlacement = (BlobPlacement) source.getPlacement();
        BlobPlacement destPlacement = (BlobPlacement) dest.getPlacement();
        ConsistencyLevel consistency = CONSISTENCY_STRONG;

        // Scan through the row metadata, skipping the chunk columns for now.
        CompositeSerializer colSerializer = CompositeSerializer.get();
        ByteBufferRange metadataColumnRange = new RangeBuilder()
                .setStart(getColumnPrefix(ColumnGroup.A, Composite.ComponentEquality.LESS_THAN_EQUAL), colSerializer)
                .setEnd(getColumnPrefix(ColumnGroup.B, Composite.ComponentEquality.GREATER_THAN_EQUAL), colSerializer)
                .build();
        Iterator<List<Row<ByteBuffer, Composite>>> rowsIter = Iterators.partition(
                scanInternal(sourcePlacement, keyRange, metadataColumnRange, LimitCounter.max()),
                MAX_SCAN_METADATA_BATCH_SIZE);

        while (rowsIter.hasNext()) {
            List<Row<ByteBuffer, Composite>> rows = rowsIter.next();

            MutationBatch summaryMutation = destPlacement.getKeyspace().prepareMutationBatch(consistency);
            for (Row<ByteBuffer, Composite> row : rows) {
                // Map the source row key to the destination row key.  Its table uuid and shard key will be different.
                ByteBuffer newRowKey = dest.getRowKey(AstyanaxStorage.getContentKey(row.getRawKey()));

                for (Column<Composite> column : row.getColumns()) {
                    Composite name = column.getName();
                    ColumnGroup group = ColumnGroup.valueOf(name.get(0, AsciiSerializer.get()));
                    int chunkId = name.get(1, IntegerSerializer.get());

                    if (copyMetadata && group == ColumnGroup.A) {
                        // Found a blob summary.  Copy the summaries for multiple rows together in a batch.
                        summaryMutation.withRow(destPlacement.getBlobColumnFamily(), newRowKey)
                                .setTimestamp(column.getTimestamp())
                                .putColumn(name, column.getByteBufferValue(), column.getTtl());

                    } else if (copyData && group == ColumnGroup.B) {
                        // Found a chunk presence column.  Fetch and copy the chunk data, one chunk at a time.
                        // Make sure chunk presence columns and data columns are paired together at all times.
                        ColumnQuery<Composite> query = sourcePlacement.getKeyspace()
                                .prepareQuery(sourcePlacement.getBlobColumnFamily(), consistency)
                                .getKey(row.getRawKey())
                                .getColumn(getColumn(ColumnGroup.Z, chunkId));
                        Column<Composite> chunk;
                        try {
                            chunk = query.execute().getResult();
                        } catch (NotFoundException e) {
                            continue;  // Unusual, but possible if racing a delete.
                        } catch (ConnectionException e) {
                            throw Throwables.propagate(e);
                        }

                        // Write two columns: one small one and one big one with the binary data.  Readers can query on
                        // the presence of the small one to be confident that the big column has replicated and is available.
                        MutationBatch chunkMutation = destPlacement.getKeyspace().prepareMutationBatch(consistency);
                        chunkMutation.withRow(destPlacement.getBlobColumnFamily(), newRowKey)
                                .setTimestamp(chunk.getTimestamp())
                                .putEmptyColumn(getColumn(ColumnGroup.B, chunkId), chunk.getTtl())
                                .putColumn(getColumn(ColumnGroup.Z, chunkId), chunk.getByteBufferValue(), chunk.getTtl());
                        progress.run();
                        execute(chunkMutation);
                    }
                }
            }
            if (!summaryMutation.isEmpty()) {
                progress.run();
                execute(summaryMutation);
                _blobMetadataCopyMeter.mark(summaryMutation.getRowCount());
            }

            _blobCopyMeter.mark(rows.size());
        }
    }

    // DataPurgeDAO
    @Override
    public void purge(AstyanaxStorage storage, Runnable progress) {
//        metadata purge will be conditional for s3 migration
        purge(storage, true, true, progress);
    }

    private void purge(AstyanaxStorage storage, boolean deleteMetadata, boolean deleteData, Runnable progress) {
        BlobPlacement placement = (BlobPlacement) storage.getPlacement();
        CassandraKeyspace keyspace = placement.getKeyspace();
        ColumnFamily<ByteBuffer, Composite> cf = placement.getBlobColumnFamily();

        // Limit the query to a single column since we mainly just want the row keys (but not zero columns because
        // then we couldn't distinguish a live row from a row that has been deleted already).
        ByteBufferRange columnRange = new RangeBuilder().setLimit(1).build();

        MutationBatch mutation = keyspace.prepareMutationBatch(CONSISTENCY_STRONG);

        LimitCounter unlimited = LimitCounter.max();

        // Range query all the shards and delete all the rows we find.
        Iterator<ByteBufferRange> scanIter = storage.scanIterator(null);
        while (scanIter.hasNext()) {
            ByteBufferRange keyRange = scanIter.next();
            Iterator<Row<ByteBuffer, Composite>> rowIter = scanInternal(placement, keyRange, columnRange, unlimited);
            while (rowIter.hasNext()) {
                Row<ByteBuffer, Composite> row = rowIter.next();
                if (row.getColumns().isEmpty()) {
                    continue;  // don't bother deleting range ghosts
                }

                if (deleteMetadata && deleteData) {
                    mutation.withRow(cf, row.getKey()).delete();
                } else {
                    if (deleteMetadata) {
                        mutation.withRow(cf, row.getKey())
                                .deleteColumn(getColumn(ColumnGroup.A, 0));
                    }

                    if (deleteData) {
                        mutation.withRow(cf, row.getKey())
                                .deleteColumn(getColumn(ColumnGroup.B, 1))
                                .deleteColumn(getColumn(ColumnGroup.Z, 2));
                    }
                }

                if (mutation.getRowCount() >= 100) {
                    progress.run();
                    execute(mutation);
                    mutation.discardMutations();
                }
            }
        }
        if (!mutation.isEmpty()) {
            progress.run();
            execute(mutation);
        }
    }

    /**
     * Queries for rows within the specified range, exclusive on start and inclusive on end.
     */
    private Iterator<Row<ByteBuffer, Composite>> scanInternal(final BlobPlacement placement, final ByteBufferRange keyRange,
                                                              final ByteBufferRange columnRange, final LimitCounter limit) {
        return Iterators.concat(new AbstractIterator<Iterator<Row<ByteBuffer, Composite>>>() {
            private ByteBuffer _rangeStart = keyRange.getStart();
            private final ByteBuffer _rangeEnd = keyRange.getEnd();
            private int _minimumLimit = 1;
            private boolean _done;

            @Override
            protected Iterator<Row<ByteBuffer, Composite>> computeNext() {
                // Note: if Cassandra is asked to perform a token range query where start >= end it will wrap
                // around which is absolutely *not* what we want since it could return data for another table.
                if (_done || BufferUtils.compareUnsigned(_rangeStart, _rangeEnd) >= 0) {
                    return endOfData();
                }

                Timer.Context timer = _scanBatchTimer.time();
                try {
                    int batchSize = (int) Math.min(Math.max(limit.remaining(), _minimumLimit), MAX_SCAN_METADATA_BATCH_SIZE);
                    // Increase the minimum limit a bit each time around so if we start encountering lots of range
                    // ghosts we eventually scan through them at a reasonable rate.
                    _minimumLimit = Math.min(_minimumLimit + 3, MAX_SCAN_METADATA_BATCH_SIZE);

                    // Pass token strings to get exclusive start behavior, to support 'fromBlobIdExclusive'.
                    Rows<ByteBuffer, Composite> rows = execute(placement.getKeyspace()
                            .prepareQuery(placement.getBlobColumnFamily(), _readConsistency)
                            .getKeyRange(null, null, toTokenString(_rangeStart), toTokenString(_rangeEnd), batchSize)
                            .withColumnRange(columnRange));

                    if (rows.size() >= batchSize) {
                        // Save the last row key so we can use it as the start (exclusive) if we must query to get more data.
                        _rangeStart = rows.getRowByIndex(rows.size() - 1).getKey();
                    } else {
                        // If we got fewer rows than we asked for, another query won't find more rows.
                        _done = true;
                    }

                    // Track metrics
                    _scanReadMeter.mark(rows.size());

                    // Return the rows.  Filter out range ghosts (deleted rows with no columns)
                    final Iterator<Row<ByteBuffer, Composite>> rowIter = rows.iterator();
                    return new AbstractIterator<Row<ByteBuffer, Composite>>() {
                        @Override
                        protected Row<ByteBuffer, Composite> computeNext() {
                            while (rowIter.hasNext()) {
                                Row<ByteBuffer, Composite> row = rowIter.next();
                                if (!row.getColumns().isEmpty()) {
                                    return row;
                                }
                            }
                            return endOfData();
                        }
                    };
                } finally {
                    timer.stop();
                }
            }
        });
    }

    @Override
    public int getDefaultChunkSize() {
        return DEFAULT_CHUNK_SIZE;
    }

    private static void deleteDataColumns(AstyanaxTable table, String blobId, ColumnList<Composite> columns, ConsistencyLevel consistency, Long timestamp) {
        for (AstyanaxStorage storage : table.getWriteStorage()) {
            BlobPlacement placement = (BlobPlacement) storage.getPlacement();

            // Any columns with a timestamp older than the one we expect must be from an old version
            // of the blob.  This should be rare, but if it happens clean up and delete the old data.
            MutationBatch mutation = placement.getKeyspace().prepareMutationBatch(consistency);
            ColumnListMutation<Composite> row = mutation.withRow(
                    placement.getBlobColumnFamily(), storage.getRowKey(blobId));
            boolean found = false;
            for (Column<Composite> column : columns) {
                if (null != timestamp && column.getTimestamp() < timestamp) {
                    if (ColumnGroup.B.name().equals(column.getName().get(0, AsciiSerializer.get()))) {
                        int chunkId = column.getName().get(1, IntegerSerializer.get());
                        row.deleteColumn(getColumn(ColumnGroup.B, chunkId))
                                .deleteColumn(getColumn(ColumnGroup.Z, chunkId));
                        found = true;
                    }
                }
            }
            if (found) {
                execute(mutation);
            }
        }
    }

    private static Composite getColumn(ColumnGroup group, int index) {
        Composite column = new Composite();
        column.addComponent(group.name(), AsciiSerializer.get());
        column.addComponent(index, IntegerSerializer.get());
        return column;
    }

    private static Composite getColumnPrefix(ColumnGroup group, Composite.ComponentEquality equality) {
        Composite column = new Composite();
        column.addComponent(group.name(), AsciiSerializer.get(), equality);
        return column;
    }

    /**
     * The Astyanax Composite behavior is broken in that a deserialized Composite is not .equal() to a normally
     * created Composite because the serializers aren't used correctly.  This function works around the problem.
     */
    private static boolean matches(Composite column, ColumnGroup group, int index) {
        return column.size() == 2 &&
                column.get(0, AsciiSerializer.get()).equals(group.name()) &&
                column.get(1, IntegerSerializer.get()).equals(index);
    }

    private String toTokenString(ByteBuffer bytes) {
        return _tokenFactory.toString(_tokenFactory.fromByteArray(bytes));
    }

    private static <R> R execute(Execution<R> execution) {
        OperationResult<R> operationResult;
        try {
            operationResult = execution.execute();
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }
        return operationResult.getResult();
    }

    /**
     * Force computation of the first item in an iterator so metrics calculations for a method reflect the cost of
     * the first batch of results.
     */
    private static <T> Iterator<T> touch(Iterator<T> iter) {
        // Could return a Guava PeekingIterator after "if (iter.hasNext()) iter.peek()", but simply calling hasNext()
        // is sufficient for the iterator implementations used by this DAO class...
        iter.hasNext();
        return iter;
    }
}
