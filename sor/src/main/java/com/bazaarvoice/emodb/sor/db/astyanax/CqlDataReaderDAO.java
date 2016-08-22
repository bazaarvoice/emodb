package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.db.DataReaderDAO;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.MultiTableScanOptions;
import com.bazaarvoice.emodb.sor.db.MultiTableScanResult;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.db.RecordEntryRawMetadata;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.db.ScanRangeSplits;
import com.bazaarvoice.emodb.sor.db.cql.CachingResultSet;
import com.bazaarvoice.emodb.sor.db.cql.CqlReaderDAODelegate;
import com.bazaarvoice.emodb.sor.db.cql.ResultSetSupplier;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxTable;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

// Delegates to AstyanaxReaderDAO for non-CQL stuff
// Once we transition fully, we will stop delegating to Astyanax
public class CqlDataReaderDAO implements DataReaderDAO {
    private static final int MAX_RANDOM_ROWS_BATCH = 50;
    private static final int MIN_BATCH_FETCH_SIZE = 500;

    private final DataReaderDAO _astyanaxReaderDAO;
    private final ChangeEncoder _changeEncoder;
    private final PlacementCache _placementCache;
    private final Meter _randomReadMeter;
    private final Timer _readBatchTimer;

    private volatile boolean _alwaysDelegateToAstyanax = false;
    private volatile int _fetchSize = 50;
    private volatile int _batchFetchSize = MIN_BATCH_FETCH_SIZE;
    private volatile boolean _asyncCqlRandomSeeks = false;

    @Inject
    public CqlDataReaderDAO(@CqlReaderDAODelegate DataReaderDAO delegate, PlacementCache placementCache, MetricRegistry metricRegistry) {
        _astyanaxReaderDAO = checkNotNull(delegate, "delegate");
        _placementCache = placementCache;
        _changeEncoder = new DefaultChangeEncoder();
        _randomReadMeter = metricRegistry.meter(getMetricName("random-reads"));
        _readBatchTimer = metricRegistry.timer(getMetricName("readBatch"));
    }

    private String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.sor", "CqlDataReaderDAO", name);
    }

    /**
     * This CQL based read method works for a row with 64 deltas of 3 MB each. The same read with the AstyanaxDataReaderDAO
     * would give Thrift frame errors.
     */
    @Override
    public Record read(Key key, ReadConsistency consistency) {
        if (_alwaysDelegateToAstyanax) {
            return _astyanaxReaderDAO.read(key, consistency);
        }
        checkNotNull(key, "key");
        checkNotNull(consistency, "consistency");

        AstyanaxTable table = (AstyanaxTable) key.getTable();
        AstyanaxStorage storage = table.getReadStorage();
        DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();
        ByteBuffer rowKey = storage.getRowKey(key.getKey());

        return read(key, rowKey, consistency, placement);
    }

    @Override
    public Iterator<Record> readAll(Collection<Key> keys, final ReadConsistency consistency) {
        if (_alwaysDelegateToAstyanax) {
            return _astyanaxReaderDAO.readAll(keys, consistency);
        }

        checkNotNull(keys, "keys");
        checkNotNull(consistency, "consistency");

        // Group the keys by placement.  Each placement will result in a separate set of queries.  Dedup keys.
        Multimap<DeltaPlacement, Key> placementMap = HashMultimap.create();
        for (Key key : keys) {
            AstyanaxTable table = (AstyanaxTable) key.getTable();
            AstyanaxStorage storage = table.getReadStorage();
            placementMap.put((DeltaPlacement) storage.getPlacement(), key);
        }

        // Return an iterator that will loop over the placements and perform a query for each placement and
        // return the resulting decoded rows.
        return touch(Iterators.concat(Iterators.transform(placementMap.asMap().entrySet().iterator(),
                new Function<Map.Entry<DeltaPlacement, Collection<Key>>, Iterator<Record>>() {
                    @Override
                    public Iterator<Record> apply(Map.Entry<DeltaPlacement, Collection<Key>> entry) {
                        return readBatch(entry.getKey(), entry.getValue(), consistency);
                    }
                })));
    }

    @Override
    public String getPlacementCluster(String placementName) {
        checkNotNull(placementName, "placement");

        DeltaPlacement placement = (DeltaPlacement) _placementCache.get(placementName);
        return placement.getKeyspace().getClusterName();
    }

    public void setAlwaysDelegateToAstyanax(boolean delegateToAstyanax) {
        _alwaysDelegateToAstyanax = delegateToAstyanax;
    }

    public boolean getDelegateToAstyanax() {
        return _alwaysDelegateToAstyanax;
    }

    public void setAsyncCqlRandomSeeks(boolean asyncCqlRandomSeeks) {
        _asyncCqlRandomSeeks = asyncCqlRandomSeeks;
    }

    public boolean getAsyncCqlRandomSeeks() {
        return _asyncCqlRandomSeeks;
    }

    public int getFetchSize() {
        return _fetchSize;
    }

    public int getBatchFetchSize() {
        return _batchFetchSize;
    }

    public void setFetchSize(int fetchSize) {
        _fetchSize = fetchSize;
        _batchFetchSize = Math.max(MIN_BATCH_FETCH_SIZE, fetchSize);
    }

    private Record read(Key key, ByteBuffer rowKey, ReadConsistency consistency, DeltaPlacement placement) {
        checkNotNull(key, "key");
        checkNotNull(consistency, "consistency");

        Statement statement = QueryBuilder.select().column(placement.getDeltaRowKeyColumnName())
                .column(placement.getDeltaColumnName()).column(placement.getDeltaValueColumnName())
                .from(placement.getDeltaTableMetadata())
                .where(eq(placement.getDeltaRowKeyColumnName(), rowKey))
                .setConsistencyLevel(SorConsistencies.toCql(consistency));

        // Samples taken with different fetch sizes locally for 3 MB deltas (times only used for comparison)
        // Fetch size of 10 took 6.058s of CPU time
        // Fetch size of 5 took 5.447s of CPU time
        // Fetch size of 1 took 6.568s of CPU time

        // For our purposes, setting this value to 10 should do fine.
        statement.setFetchSize(_fetchSize);

        Session session = placement.getKeyspace().getCqlSession();

        // Track metrics
        _randomReadMeter.mark();

        // Convert the results into a Record object, lazily fetching the rest of the columns as necessary.
        return newRecordFromCql(key, new ResultSetSupplier(session, statement, false));
    }

    private Record newRecordFromCql(Key key, ResultSetSupplier resultSetSupplier) {
        CachingResultSet cachingResultSet = new CachingResultSet(resultSetSupplier);

        Iterator<Map.Entry<UUID, Change>> changeIter = decodeChangesFromCql(cachingResultSet.iterator());
        Iterator<Map.Entry<UUID, Compaction>> compactionIter = decodeCompactionsFromCql(cachingResultSet.iterator());
        Iterator<RecordEntryRawMetadata> rawMetadataIter = rawMetadataFromCql(cachingResultSet.iterator());

        return new RecordImpl(key, compactionIter, changeIter, rawMetadataIter);
    }

    private Iterator<Map.Entry<UUID, Change>> decodeChangesFromCql(final Iterator<com.datastax.driver.core.Row> iter) {
        return Iterators.transform(iter, new Function<Row, Map.Entry<UUID, Change>>() {
            @Override
            public Map.Entry<UUID, Change> apply(com.datastax.driver.core.Row column) {
                Change change = _changeEncoder.decodeChange(column.getUUID(1), column.getBytesUnsafe(2));
                return Maps.immutableEntry(column.getUUID(1), change);
            }
        });
    }

    private Iterator<Map.Entry<UUID, Compaction>> decodeCompactionsFromCql(final Iterator<com.datastax.driver.core.Row> iter) {
        return new AbstractIterator<Map.Entry<UUID, Compaction>>() {
            @Override
            protected Map.Entry<UUID, Compaction> computeNext() {
                while (iter.hasNext()) {
                    com.datastax.driver.core.Row column = iter.next();
                    Compaction compaction = _changeEncoder.decodeCompaction(column.getBytesUnsafe(2));
                    if (compaction != null) {
                        return Maps.immutableEntry(column.getUUID(1), compaction);
                    }
                }
                return endOfData();
            }
        };
    }

    private Iterator<RecordEntryRawMetadata> rawMetadataFromCql(final Iterator<com.datastax.driver.core.Row> iter) {
        return Iterators.transform(iter, new Function<com.datastax.driver.core.Row, RecordEntryRawMetadata>() {
            @Override
            public RecordEntryRawMetadata apply(com.datastax.driver.core.Row column) {
                return new RecordEntryRawMetadata()
                        .withTimestamp(TimeUUIDs.getTimeMillis(column.getUUID(1)))
                        .withSize(column.getBytesUnsafe(2).remaining());
            }
        });
    }

    /** Read a batch of keys that all belong to the same placement (ColumnFamily). */
    private Iterator<Record> readBatch(final DeltaPlacement placement, final Collection<Key> keys, final ReadConsistency consistency) {


        checkNotNull(keys, "keys");

        // Convert the keys to ByteBuffer Cassandra row keys
        List<Map.Entry<ByteBuffer, Key>> rowKeys = Lists.newArrayListWithCapacity(keys.size());
        for (Key key : keys) {
            AstyanaxTable table = (AstyanaxTable) key.getTable();
            AstyanaxStorage storage = table.getReadStorage();
            rowKeys.add(Maps.immutableEntry(storage.getRowKey(key.getKey()), key));
        }

        // Sort the keys by their byte array encoding to get some locality w/queries.
        Collections.sort(rowKeys, Ordering.natural().onResultOf(entryKeyFunction()));

        // Group them into batches.  Cassandra may have to seek each row so prefer smaller batches.
        List<List<Map.Entry<ByteBuffer, Key>>> batches = Lists.partition(rowKeys, MAX_RANDOM_ROWS_BATCH);

        // This algorithm is arranged such that only one row of raw decoded changes is pinned in memory at a time.
        // If there are lots of rows with large #s of deltas our memory use should be bounded by the size of the
        // single row with the _fetchSize number of deltas.

        return Iterators.concat(Iterators.transform(batches.iterator(),
                new Function<List<Map.Entry<ByteBuffer, Key>>, Iterator<Record>>() {
                    @Override
                    public Iterator<Record> apply(List<Map.Entry<ByteBuffer, Key>> rowKeys) {
                        Timer.Context timerCtx = _readBatchTimer.time();
                        try {
                            if (_asyncCqlRandomSeeks) {
                                return rowQueryAsync(rowKeys, consistency, placement);
                            } else {
                                return rowQuerySync(rowKeys, consistency, placement);
                            }
                        } finally {
                            timerCtx.stop();
                        }
                    }
                }));
    }

    /** Uses multiple async calls to cassandra for each key. */
    private Iterator<Record> rowQueryAsync(final List<Map.Entry<ByteBuffer, Key>> rowKeys, final ReadConsistency consistency,
                                           final DeltaPlacement placement) {
        final Session session = placement.getKeyspace().getCqlSession();

        // Create ResultSetSupplier for each key eagerly so that we execute all queries asynchronously
        List<Map.Entry<Key, ResultSetSupplier>> resultSetSuppliers = Lists.transform(rowKeys, new Function<Map.Entry<ByteBuffer, Key>,
                Map.Entry<Key, ResultSetSupplier>>() {
            @Nullable
            @Override
            public Map.Entry<Key, ResultSetSupplier> apply(Map.Entry<ByteBuffer, Key> input) {
                Statement statement = QueryBuilder.select().column(placement.getDeltaRowKeyColumnName())
                        .column(placement.getDeltaColumnName()).column(placement.getDeltaValueColumnName())
                        .from(placement.getDeltaTableMetadata())
                        .where(eq(placement.getDeltaRowKeyColumnName(), input.getKey()))
                        .setConsistencyLevel(SorConsistencies.toCql(consistency));

                statement.setFetchSize(_fetchSize);
                return Maps.immutableEntry(input.getValue(), new ResultSetSupplier(session, statement, true));
            }
        });

        return Iterables.transform(resultSetSuppliers, new Function<Map.Entry<Key, ResultSetSupplier>, Record>() {
            @Override
            public Record apply(Map.Entry<Key, ResultSetSupplier> input) {
                return newRecordFromCql(input.getKey(), input.getValue());
            }
        }).iterator();
    }

    /** Uses one synchronous call to cassandra per batch using CQL "IN" statement. */
    @VisibleForTesting
    protected Iterator<Record> rowQuerySync(final List<Map.Entry<ByteBuffer, Key>> rowKeys, final ReadConsistency consistency,
                                            final DeltaPlacement placement) {
        final Map<ByteBuffer, Key> rowKeyMap = Maps.newHashMap();
        for (Map.Entry<ByteBuffer, Key> rowKey : rowKeys) {
            rowKeyMap.put(rowKey.getKey(), rowKey.getValue());
        }

        final List<ByteBuffer> keys = Lists.transform(rowKeys, entryKeyFunction());
        final Statement statement = QueryBuilder.select().column(placement.getDeltaRowKeyColumnName())
                .column(placement.getDeltaColumnName()).column(placement.getDeltaValueColumnName())
                .from(placement.getDeltaTableMetadata())
                .where(in(placement.getDeltaRowKeyColumnName(), keys))
                .setConsistencyLevel(SorConsistencies.toCql(consistency));

        statement.setFetchSize(_batchFetchSize);

        final Session session = placement.getKeyspace().getCqlSession();

        final CachingResultSet cachingResultSet = new CachingResultSet(new ResultSetSupplier(session, statement, false));
        final PeekingIterator<Row> compactionCachingIterator = Iterators.peekingIterator(cachingResultSet.iterator());
        final PeekingIterator<Row> changesCachingIterator = Iterators.peekingIterator(cachingResultSet.iterator());
        final PeekingIterator<Row> rawCachingIterator = Iterators.peekingIterator(cachingResultSet.iterator());

        return new AbstractIterator<Record>() {
            @Override
            protected Record computeNext() {
                // Every Record will always go through changesCachingIterator. So, use it to determine the cursor for
                // the next record.
                if (!changesCachingIterator.hasNext()) {
                    // Before we leave, check if we have any non-existent keys left
                    if (rowKeyMap.isEmpty()) {
                        return endOfData();
                    }
                    // The rest of the keys do not exist (We should return empty records for these anyways)
                    return emptyRecord(rowKeyMap.remove(rowKeyMap.keySet().iterator().next()));
                }
                // Peek the key
                // Algorithm note: Every time computeNext() on this iterator is called,
                // changesCachingIterator would have moved to the last row of the previous record.
                ByteBuffer ckey = changesCachingIterator.peek().getBytes(0);
                // Remove the keys as we see them
                Key key = rowKeyMap.remove(ckey);

                // Precondition: Records in this iterator should *not* be resolved out of order.
                // In practice, this may happen if someone runs the following code while doing a multi-get
                // Iterator<Record> records = readAll(...);
                // records.next(); // Note that this record is not resolved, meaning the iterators are not advanced
                // resolve(records.next()); // Resolving the next record (out of order) would actually fetch previous record rows.
                // The above boils down to that every new key fetched should never equal to the one we have previously seen.
                checkState(key != null, "Out of order resolving of records detected. Must resolve records in the order they are received");

                Iterator<Map.Entry<UUID, Compaction>> compactionIter = decodeCompactionsFromCql(getCqlRowsPerKey(ckey,
                        compactionCachingIterator));
                Iterator<Map.Entry<UUID, Change>> changesIter = decodeChangesFromCql(getCqlRowsPerKey(ckey,
                        changesCachingIterator));
                Iterator<RecordEntryRawMetadata> rawMetaIter = rawMetadataFromCql(getCqlRowsPerKey(ckey,
                        rawCachingIterator));
                return new RecordImpl(key, compactionIter, changesIter, rawMetaIter);
            }
        };
    }

    // The following helper method utilizes the same ResultSet's iterator for multiple keys, instead of creating one
    // resultset for each key.
    private Iterator<Row> getCqlRowsPerKey(@NotNull final ByteBuffer rowKey, final PeekingIterator<Row> cachingIterator) {
        return new AbstractIterator<Row>() {
            private boolean checkForOutOfOrderFetch = true;
            protected Row computeNext() {
                if (cachingIterator.hasNext()) {
                    Row cqlRow = cachingIterator.peek();
                    ByteBuffer currKey = cqlRow.getBytes(0);
                    // Check only for the first row. If checks out, then don't check again.
                    checkState(!checkForOutOfOrderFetch || currKey.equals(rowKey),
                            "Should always get a new rowkey for a new iterator.");
                    checkForOutOfOrderFetch = false;
                    if (!currKey.equals(rowKey)) {
                        // rowKey has changed
                        return endOfData();
                    }
                    return cachingIterator.next();
                }
                return endOfData();
            }
        };
    }

    private <T> Iterator<T> touch(Iterator<T> iter) {
        // Could return a Guava PeekingIterator after "if (iter.hasNext()) iter.peek()", but simply calling hasNext()
        // is sufficient for the iterator implementations used by this DAO class...
        iter.hasNext();
        return iter;
    }

    private Function<Map.Entry<ByteBuffer, Key>, ByteBuffer> entryKeyFunction() {
        return new Function<Map.Entry<ByteBuffer, Key>, ByteBuffer>() {
            @Override
            public ByteBuffer apply(Map.Entry<ByteBuffer, Key> entry) {
                return entry.getKey();
            }
        };
    }

    private Record emptyRecord(Key key) {
        return new RecordImpl(key,
                Iterators.<Map.Entry<UUID, Compaction>>emptyIterator(),
                Iterators.<Map.Entry<UUID, Change>>emptyIterator(),
                Iterators.<RecordEntryRawMetadata>emptyIterator());
    }


    // The following methods delegate to AsytanaxDataReaderDAO

    @Override
    public long count(Table table, ReadConsistency consistency) {
        return _astyanaxReaderDAO.count(table, consistency);
    }

    @Override
    public long count(Table table, @Nullable Integer limit, ReadConsistency consistency) {
        return _astyanaxReaderDAO.count(table, limit, consistency);
    }

    @Override
    public Iterator<Change> readTimeline(Key key, boolean includeContentData, boolean includeAuditInformation, UUID start, UUID end, boolean reversed, long limit, ReadConsistency consistency) {
        return _astyanaxReaderDAO.readTimeline(key, includeContentData, includeAuditInformation, start, end, reversed, limit, consistency);
    }

    @Override
    public Iterator<Change> getExistingAudits(Key key, UUID start, UUID end, ReadConsistency consistency) {
        return _astyanaxReaderDAO.getExistingAudits(key, start, end, consistency);
    }

    @Override
    public Iterator<Record> scan(Table table, @Nullable String fromKeyExclusive, LimitCounter limit, ReadConsistency consistency) {
        return _astyanaxReaderDAO.scan(table, fromKeyExclusive, limit, consistency);
    }

    @Override
    public List<String> getSplits(Table table, int desiredRecordsPerSplit) {
        return _astyanaxReaderDAO.getSplits(table, desiredRecordsPerSplit);
    }

    @Override
    public Iterator<Record> getSplit(Table table, String split, @Nullable String fromKeyExclusive, LimitCounter limit, ReadConsistency consistency) {
        return _astyanaxReaderDAO.getSplit(table, split, fromKeyExclusive, limit, consistency);
    }

    @Override
    public ScanRangeSplits getScanRangeSplits(String placement, int desiredRecordsPerSplit, Optional<ScanRange> subrange) {
        return _astyanaxReaderDAO.getScanRangeSplits(placement, desiredRecordsPerSplit, subrange);
    }

    @Override
    public Iterator<MultiTableScanResult> multiTableScan(MultiTableScanOptions query, TableSet tables, LimitCounter limit, ReadConsistency consistency) {
        return _astyanaxReaderDAO.multiTableScan(query, tables, limit, consistency);
    }
}
