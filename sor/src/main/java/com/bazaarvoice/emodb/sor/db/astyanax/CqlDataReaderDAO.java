package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.db.DataReaderDAO;
import com.bazaarvoice.emodb.sor.db.Key;
import com.bazaarvoice.emodb.sor.db.MultiTableScanOptions;
import com.bazaarvoice.emodb.sor.db.MultiTableScanResult;
import com.bazaarvoice.emodb.sor.db.Record;
import com.bazaarvoice.emodb.sor.db.RecordEntryRawMetadata;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.sor.db.ScanRangeSplits;
import com.bazaarvoice.emodb.sor.db.cql.CachingRowGroupIterator;
import com.bazaarvoice.emodb.sor.db.cql.CqlReaderDAODelegate;
import com.bazaarvoice.emodb.sor.db.cql.RowGroupResultSetIterator;
import com.bazaarvoice.emodb.table.db.DroppedTableException;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxTable;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.Timed;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.BoundType;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Range;
import com.google.inject.Inject;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.util.ByteBufferRangeImpl;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.asc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.desc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.token;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

// Delegates to AstyanaxReaderDAO for non-CQL stuff
// Once we transition fully, we will stop delegating to Astyanax
public class CqlDataReaderDAO implements DataReaderDAO {
    private static final int MAX_RANDOM_ROWS_BATCH = 50;
    private static final int DEFAULT_SINGLE_ROW_FETCH_SIZE = 100;
    private static final int DEFAULT_SINGLE_ROW_PREFETCH_LIMIT = 50;
    private static final int DEFAULT_MULTI_ROW_FETCH_SIZE = 500;
    private static final int DEFAULT_MULTI_ROW_PREFETCH_LIMIT = 200;
    private static final int RECORD_CACHE_SIZE = 20;
    private static final int RECORD_SOFT_CACHE_SIZE = 10;

    private final DataReaderDAO _astyanaxReaderDAO;
    private final ChangeEncoder _changeEncoder;
    private final PlacementCache _placementCache;
    private final Meter _randomReadMeter;
    private final Timer _readBatchTimer;

    private volatile boolean _alwaysDelegateToAstyanax = false;
    private volatile boolean _asyncCqlRandomSeeks = false;
    private volatile int _singleRowFetchSize = DEFAULT_SINGLE_ROW_FETCH_SIZE;
    private volatile int _multiRowFetchSize = DEFAULT_MULTI_ROW_FETCH_SIZE;
    private volatile int _singleRowPrefetchLimit = DEFAULT_SINGLE_ROW_PREFETCH_LIMIT;
    private volatile int _multiRowPrefetchLimit = DEFAULT_MULTI_ROW_PREFETCH_LIMIT;

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

    public void setSingleRowFetchSizeAndPrefetchLimit(int singleRowFetchSize, int singleRowPrefetchLimit) {
        checkArgument(singleRowFetchSize > 0, "Fetch size must be greater than 0");
        checkArgument(singleRowPrefetchLimit >= 0, "Prefetch limit must be at least 0");
        checkArgument(singleRowPrefetchLimit < singleRowFetchSize, "Fetch size cannot be lower than prefetch limit");
        _singleRowFetchSize = singleRowFetchSize;
        _singleRowPrefetchLimit = singleRowPrefetchLimit;
    }

    public void setMultiRowFetchSizeAndPrefetchLimit(int multiRowFetchSize, int multiRowPrefetchLimit) {
        checkArgument(multiRowFetchSize > 0, "Fetch size must be greater than 0");
        checkArgument(multiRowPrefetchLimit >= 0, "Prefetch limit must be at least 0");
        checkArgument(multiRowPrefetchLimit < multiRowFetchSize, "Fetch size cannot be lower than prefetch limit");
        _multiRowFetchSize = multiRowFetchSize;
        _multiRowPrefetchLimit = multiRowPrefetchLimit;
    }

    public int getSingleRowFetchSize() {
        return _singleRowFetchSize;
    }

    public int getMultiRowFetchSize() {
        return _multiRowFetchSize;
    }

    public int getSingleRowPrefetchLimit() {
        return _singleRowPrefetchLimit;
    }

    public int getMultiRowPrefetchLimit() {
        return _multiRowPrefetchLimit;
    }

    private Record read(Key key, ByteBuffer rowKey, ReadConsistency consistency, DeltaPlacement placement) {
        checkNotNull(key, "key");
        checkNotNull(consistency, "consistency");

        TableDDL tableDDL = placement.getDeltaTableDDL();

        Statement statement = QueryBuilder.select().column(tableDDL.getRowKeyColumnName())
                .column(tableDDL.getChangeIdColumnName()).column(tableDDL.getValueColumnName())
                .from(tableDDL.getTableMetadata())
                .where(eq(tableDDL.getRowKeyColumnName(), rowKey))
                .setConsistencyLevel(SorConsistencies.toCql(consistency));


        // Track metrics
        _randomReadMeter.mark();

        Iterator<Iterable<Row>> groupedRows = deltaQuery(placement, statement, true);

        Iterable<Row> rows;
        if (groupedRows.hasNext()) {
            rows = groupedRows.next();
        } else {
            rows = ImmutableList.of();
        }

        // Convert the results into a Record object, lazily fetching the rest of the columns as necessary.
        return newRecordFromCql(key, rows);
    }

    /**
     * Synchronously executes the provided statement.  The statement must query the delta table as returned from
     * {@link com.bazaarvoice.emodb.sor.db.astyanax.DeltaPlacement#getDeltaTableDDL()}
     */
    private Iterator<Iterable<Row>> deltaQuery(DeltaPlacement placement, Statement statement, boolean singleRow) {
        return doDeltaQuery(placement, statement, singleRow, false);
    }

    /**
     * Asynchronously executes the provided statement.  Although the iterator is returned immediately the actual results
     * may still be loading in the background.  The statement must query the delta table as returned from
     * {@link com.bazaarvoice.emodb.sor.db.astyanax.DeltaPlacement#getDeltaTableDDL()}
     */
    private Iterator<Iterable<Row>> deltaQueryAsync(DeltaPlacement placement, Statement statement, boolean singleRow) {
        return doDeltaQuery(placement, statement, singleRow, true);
    }

    private Iterator<Iterable<Row>> doDeltaQuery(DeltaPlacement placement, Statement statement, boolean singleRow, boolean async) {
        // Set the fetch size and prefetch limits depending on whether the query is for a single row or multiple rows.
        int fetchSize = singleRow ? _singleRowFetchSize : _multiRowFetchSize;
        int prefetchLimit = singleRow ? _singleRowPrefetchLimit : _multiRowPrefetchLimit;

        statement.setFetchSize(fetchSize);

        Session session = placement.getKeyspace().getCqlSession();
        DeltaRowGroupResultSetIterator deltaRowGroupResultSetIterator;

        if (async) {
            ResultSetFuture resultSetFuture = session.executeAsync(statement);
            deltaRowGroupResultSetIterator = new DeltaRowGroupResultSetIterator(
                    resultSetFuture, prefetchLimit, placement, statement.getConsistencyLevel());
        } else {
            ResultSet resultSet = session.execute(statement);
            deltaRowGroupResultSetIterator = new DeltaRowGroupResultSetIterator(
                    resultSet, prefetchLimit, placement, statement.getConsistencyLevel());
        }

        return new CachingRowGroupIterator(deltaRowGroupResultSetIterator, RECORD_CACHE_SIZE, RECORD_SOFT_CACHE_SIZE);
    }

    /**
     * Creates a Record instance for a given key and list of rows.  All rows must be from the same Cassandra row;
     * in other words, it is expected that row.getBytesUnsafe(0) returns the same value for each row in rows.
     */
    private Record newRecordFromCql(Key key, Iterable<Row> rows) {
        Iterator<Map.Entry<UUID, Change>> changeIter = decodeChangesFromCql(rows.iterator());
        Iterator<Map.Entry<UUID, Compaction>> compactionIter = decodeCompactionsFromCql(rows.iterator());
        Iterator<RecordEntryRawMetadata> rawMetadataIter = rawMetadataFromCql(rows.iterator());

        return new RecordImpl(key, compactionIter, changeIter, rawMetadataIter);
    }

    /**
     * Converts a list of rows into Change instances.
     */
    private Iterator<Map.Entry<UUID, Change>> decodeChangesFromCql(final Iterator<Row> iter) {
        return Iterators.transform(iter, new Function<Row, Map.Entry<UUID, Change>>() {
            @Override
            public Map.Entry<UUID, Change> apply(Row column) {
                Change change = _changeEncoder.decodeChange(column.getUUID(1), column.getBytesUnsafe(2));
                return Maps.immutableEntry(column.getUUID(1), change);
            }
        });
    }

    /**
     * Like {@link #decodeChangesFromCql(java.util.Iterator)} except filtered to only include compactions.
     */
    private Iterator<Map.Entry<UUID, Compaction>> decodeCompactionsFromCql(final Iterator<Row> iter) {
        return new AbstractIterator<Map.Entry<UUID, Compaction>>() {
            @Override
            protected Map.Entry<UUID, Compaction> computeNext() {
                while (iter.hasNext()) {
                    Row column = iter.next();
                    Compaction compaction = _changeEncoder.decodeCompaction(column.getBytesUnsafe(2));
                    if (compaction != null) {
                        return Maps.immutableEntry(column.getUUID(1), compaction);
                    }
                }
                return endOfData();
            }
        };
    }

    /**
     * Converts the rows from the provided iterator into raw metadata.
     */
    private Iterator<RecordEntryRawMetadata> rawMetadataFromCql(final Iterator<Row> iter) {
        return Iterators.transform(iter, new Function<Row, RecordEntryRawMetadata>() {
            @Override
            public RecordEntryRawMetadata apply(Row column) {
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

        // This algorithm is arranged such that rows are return in pages with size _fetchSize.  The rows are grouped
        // into row groups by common row key.  The first RECORD_CACHE_SIZE rows are cached for the row group
        // and any remaining rows are cached using soft references.  This places an upper bound on the memory
        // requirements needed while iterating.  If at any time a soft reference is lost C* is re-queried to
        // fetch the missing columns.

        return Iterators.concat(Iterators.transform(batches.iterator(),
                new Function<List<Map.Entry<ByteBuffer, Key>>, Iterator<Record>>() {
                    @Override
                    public Iterator<Record> apply(List<Map.Entry<ByteBuffer, Key>> rowKeys) {
                        Timer.Context timerCtx = _readBatchTimer.time();
                        try {
                            return rowQuery(rowKeys, consistency, placement);
                        } finally {
                            timerCtx.stop();
                        }
                    }
                }));
    }

    /**
     * Returns an iterator for the Records keyed by the provided row keys.  An empty record is returned for any
     * key which does not have a corresponding row in C*.
     */
    private Iterator<Record> rowQuery(final List<Map.Entry<ByteBuffer, Key>> rowKeys, final ReadConsistency consistency,
                                      final DeltaPlacement placement) {
        List<ByteBuffer> keys = Lists.newArrayListWithCapacity(rowKeys.size());
        final Map<ByteBuffer, Key> rawKeyMap = Maps.newHashMap();
        for (Map.Entry<ByteBuffer, Key> entry : rowKeys) {
            keys.add(entry.getKey());
            rawKeyMap.put(entry.getKey(), entry.getValue());
        }

        TableDDL tableDDL = placement.getDeltaTableDDL();

        Statement statement = QueryBuilder.select().column(tableDDL.getRowKeyColumnName())
                .column(tableDDL.getChangeIdColumnName()).column(tableDDL.getValueColumnName())
                .from(tableDDL.getTableMetadata())
                .where(in(tableDDL.getRowKeyColumnName(), keys))
                .setConsistencyLevel(SorConsistencies.toCql(consistency));

        Iterator<Iterable<Row>> rowGroups = deltaQueryAsync(placement, statement, false);

        return Iterators.concat(
                // First iterator reads the row groups found and transforms them to Records
                Iterators.transform(rowGroups, new Function<Iterable<Row>, Record>() {
                    @Override
                    public Record apply(Iterable<Row> rows) {
                        ByteBuffer keyBytes = getRawKeyFromRowGroup(rows);
                        Key key = rawKeyMap.remove(keyBytes);
                        assert key != null : "Query returned row with a key out of bound";
                        return newRecordFromCql(key, rows);
                    }
                }),
                // Second iterator returns an empty Record for each key queried but not found.
                new AbstractIterator<Record>() {
                    private Iterator<Key>_nonExistentKeyIterator;

                    @Override
                    protected Record computeNext() {
                        // Lazily return an empty record for each key not found in the previous iterator.
                        // rawKeyMap.iterator() must not be called until the first iterator is completely spent.
                        if (_nonExistentKeyIterator == null) {
                            _nonExistentKeyIterator = rawKeyMap.values().iterator();
                        }
                        return _nonExistentKeyIterator.hasNext() ?
                                emptyRecord(_nonExistentKeyIterator.next()) :
                                endOfData();
                    }
                });
    }

    /**
     * A few notes on this method:
     * <ol>
     *     <li>All rows in the row group have the same key, so choosing the first row is safe.</li>
     *     <li>The rowGroup will always contain at least one row.</li>
     *     <li>The row group has at least the first row in hard cache, so iterating to the first row will never
     *         result in a new CQL query.</li>
     * </ol>
     */
    private ByteBuffer getRawKeyFromRowGroup(Iterable<Row> rowGroup) {
        Iterator<Row> iter = rowGroup.iterator();
        // Sanity check
        checkArgument(iter.hasNext(), "Row group should never contain zero rows");
        return iter.next().getBytesUnsafe(0);
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

    @Timed(name = "bv.emodb.sor.CqlDataReaderDAO.scan", absolute = true)
    @Override
    public Iterator<Record> scan(Table tbl, @Nullable String fromKeyExclusive, final LimitCounter ignore_limit,
                                 final ReadConsistency consistency) {
        // Note:  The LimitCounter is passed in as an artifact of Astyanax batching and was used as a mechanism to
        // control paging.  The CQL driver natively performs this functionality so it is not used here.  The caller
        // will apply limit boundaries on the results from this method.
        if (_alwaysDelegateToAstyanax) {
            return  _astyanaxReaderDAO.scan(tbl, fromKeyExclusive, ignore_limit, consistency);
        }

        checkNotNull(tbl, "table");
        checkNotNull(consistency, "consistency");

        final AstyanaxTable table = (AstyanaxTable) tbl;
        AstyanaxStorage storage = table.getReadStorage();
        final DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();

        // Loop over all the range prefixes (2^shardsLog2 of them) and, for each, execute Cassandra queries to
        // page through the records with that prefix.
        final Iterator<ByteBufferRange> scanIter = storage.scanIterator(fromKeyExclusive);
        return touch(Iterators.concat(new AbstractIterator<Iterator<Record>>() {
            @Override
            protected Iterator<Record> computeNext() {
                if (scanIter.hasNext()) {
                    ByteBufferRange keyRange = scanIter.next();
                    return recordScan(placement, table, keyRange, consistency);
                }
                return endOfData();
            }
        }));
    }

    @Override
    public Iterator<Record> getSplit(Table tbl, String split, @Nullable String fromKeyExclusive, LimitCounter ignore_limit,
                                     ReadConsistency consistency) {
        // Note:  The LimitCounter is passed in as an artifact of Astyanax batching and was used as a mechanism to
        // control paging.  The CQL driver natively performs this functionality so it is not used here.  The caller
        // will apply limit boundaries on the results from this method.
        if (_alwaysDelegateToAstyanax) {
            return _astyanaxReaderDAO.getSplit(tbl, split, fromKeyExclusive, ignore_limit, consistency);
        }

        checkNotNull(tbl, "table");
        checkNotNull(split, "split");
        checkNotNull(consistency, "consistency");

        ByteBufferRange splitRange = SplitFormat.decode(split);

        AstyanaxTable table = (AstyanaxTable) tbl;
        AstyanaxStorage storage = getStorageForSplit(table, splitRange);
        DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();

        ByteBufferRange keyRange = storage.getSplitRange(splitRange, fromKeyExclusive, split);
        // The fromKeyExclusive might be equal to the end token of the split.  If so, there's nothing to return.
        if (keyRange.getStart().equals(keyRange.getEnd())) {
            return Iterators.emptyIterator();
        }

        return recordScan(placement, table, keyRange, consistency);
    }

    /**
     * Scans a range of keys and returns an iterator containing each row's columns as an iterable.
     */
    private Iterator<Iterable<Row>> rowScan(DeltaPlacement placement, ByteBufferRange keyRange, ReadConsistency consistency) {
        ByteBuffer startToken = keyRange.getStart();
        ByteBuffer endToken = keyRange.getEnd();

        // Note: if Cassandra is asked to perform a token range query where start >= end it will wrap
        // around which is absolutely *not* what we want.
        checkArgument(AstyanaxStorage.compareKeys(startToken, endToken) < 0, "Cannot scan rows which loop from maximum- to minimum-token");

        TableDDL tableDDL = placement.getDeltaTableDDL();

        Statement statement = QueryBuilder.select().column(tableDDL.getRowKeyColumnName())
                .column(tableDDL.getChangeIdColumnName()).column(tableDDL.getValueColumnName())
                .from(tableDDL.getTableMetadata())
                .where(gt(token(tableDDL.getRowKeyColumnName()), startToken))
                .and(lte(token(tableDDL.getRowKeyColumnName()), endToken))
                .setConsistencyLevel(SorConsistencies.toCql(consistency));

        return deltaQueryAsync(placement, statement, false);
    }

    /**
     * Similar to {@link #rowScan(DeltaPlacement, com.netflix.astyanax.model.ByteBufferRange, com.bazaarvoice.emodb.sor.api.ReadConsistency)}
     * except this method converts each C* row into a Record.
     */
    private Iterator<Record> recordScan(DeltaPlacement placement, AstyanaxTable table, ByteBufferRange keyRange,
                                        ReadConsistency consistency) {

        Iterator<Iterable<Row>> rowGroups = rowScan(placement, keyRange, consistency);
        return decodeRows(rowGroups, table);
    }

    /**
     * Converts rows from a single C* row to a Record.
     */
    private Iterator<Record> decodeRows(Iterator<Iterable<Row>> rowGroups, final AstyanaxTable table) {
        return Iterators.transform(rowGroups, new Function<Iterable<Row>, Record>() {
            @Override
            public Record apply(Iterable<Row> rowGroup) {
                String key = AstyanaxStorage.getContentKey(getRawKeyFromRowGroup(rowGroup));
                return newRecordFromCql(new Key(table, key), rowGroup);
            }
        });
    }

    @Override
    public Iterator<MultiTableScanResult> multiTableScan(final MultiTableScanOptions query, final TableSet tables,
                                                         final LimitCounter limit, final ReadConsistency consistency) {
        if (_alwaysDelegateToAstyanax) {
            return _astyanaxReaderDAO.multiTableScan(query, tables, limit, consistency);
        }

        checkNotNull(query, "query");
        String placementName = checkNotNull(query.getPlacement(), "placement");
        final DeltaPlacement placement = (DeltaPlacement) _placementCache.get(placementName);

        ScanRange scanRange = Objects.firstNonNull(query.getScanRange(), ScanRange.all());

        // Since the range may wrap from high to low end of the token range we need to unwrap it
        List<ScanRange> ranges = scanRange.unwrapped();

        return touch(FluentIterable.from(ranges)
                .transformAndConcat(new Function<ScanRange, Iterable<MultiTableScanResult>>() {
                    @Override
                    public Iterable<MultiTableScanResult> apply(final ScanRange rowRange) {
                        return new Iterable<MultiTableScanResult>() {
                            @Override
                            public Iterator<MultiTableScanResult> iterator() {
                                return scanMultiTableRows(
                                        tables, placement, rowRange.asByteBufferRange(), limit, query.isIncludeDeletedTables(),
                                        query.isIncludeMirrorTables(), consistency);
                            }
                        };
                    }
                })
                .iterator());

    }

    /** Decodes rows returned by scanning across tables. */
    private Iterator<MultiTableScanResult> scanMultiTableRows(
            final TableSet tables, final DeltaPlacement placement, final ByteBufferRange rowRange,
            final LimitCounter limit, final boolean includeDroppedTables, final boolean includeMirrorTables,
            final ReadConsistency consistency) {

        // Avoiding pinning multiple decoded rows into memory at once.
        return limit.limit(new AbstractIterator<MultiTableScanResult>() {
            private PeekingIterator<Iterable<Row>> _iter = Iterators.peekingIterator(
                    rowScan(placement, rowRange, consistency));

            private long _lastTableUuid = -1;
            private AstyanaxTable _table = null;
            private boolean _droppedTable;
            private boolean _primaryTable;

            @Override
            protected MultiTableScanResult computeNext() {
                while (_iter.hasNext()) {
                    Iterable<Row> rowGroup = _iter.next();

                    // Convert the results into a Record object
                    ByteBuffer rowKey = getRawKeyFromRowGroup(rowGroup);

                    long tableUuid = AstyanaxStorage.getTableUuid(rowKey);
                    if (_lastTableUuid != tableUuid) {
                        _lastTableUuid = tableUuid;
                        try {
                            _table = (AstyanaxTable) tables.getByUuid(tableUuid);
                        } catch (UnknownTableException e) {
                            _table = AstyanaxTable.createUnknown(tableUuid, placement, e.getTable());
                        } catch (DroppedTableException e) {
                            _table = AstyanaxTable.createUnknown(tableUuid, placement, e.getPriorTable());
                        }
                        _droppedTable = _table.isUnknownTable();
                        _primaryTable = _table.getReadStorage().hasUUID(tableUuid);
                    }

                    // Skip dropped and mirror tables if configured
                    if ((!includeDroppedTables && _droppedTable) || (!includeMirrorTables && !_primaryTable) ) {
                        _iter = skipToNextTable(tableUuid);
                        continue;
                    }

                    int shardId = AstyanaxStorage.getShardId(rowKey);
                    String key = AstyanaxStorage.getContentKey(rowKey);
                    Record record = newRecordFromCql(new Key(_table, key), rowGroup);
                    return new MultiTableScanResult(rowKey, shardId, tableUuid, _droppedTable, record);
                }

                return endOfData();
            }

            private PeekingIterator<Iterable<Row>> skipToNextTable(long tableUuid) {
                // Iterate over the next 10 row groups first to check for a table switch.  This avoids starting a new range
                // query if the number of rows in the undesired table is small.
                int skipLimit = 10;
                Iterable<Row> rowGroup = null;

                while (skipLimit != 0 && _iter.hasNext()) {
                    rowGroup = _iter.peek();
                    ByteBuffer rawKey = getRawKeyFromRowGroup(rowGroup);
                    long nextTableUuid = AstyanaxStorage.getTableUuid(rawKey);
                    if (nextTableUuid != tableUuid) {
                        // This is the first row of a new table
                        return _iter;
                    } else {
                        _iter.next();
                        skipLimit -= 1;
                    }
                }

                if (_iter.hasNext()) {
                    // Skip the table entirely by starting a new query on the next possible table
                    assert rowGroup != null;
                    int shardId = AstyanaxStorage.getShardId(getRawKeyFromRowGroup(rowGroup));
                    ByteBuffer nextPossibleTableStart = AstyanaxStorage.getRowKeyRaw(shardId, tableUuid + 1, "");
                    ByteBuffer end = rowRange.getEnd();

                    if (AstyanaxStorage.compareKeys(nextPossibleTableStart, end) < 0) {
                        // We haven't reached the last end boundary of the original range scan
                        ByteBufferRange updatedRange = new ByteBufferRangeImpl(nextPossibleTableStart, end, -1, false);
                        return Iterators.peekingIterator(rowScan(placement, updatedRange, consistency));
                    }
                }

                return Iterators.peekingIterator(Iterators.<Iterable<Row>>emptyIterator());
            }
        });
    }

    private AstyanaxStorage getStorageForSplit(AstyanaxTable table, ByteBufferRange splitRange) {
        // During a table move, after the internal copy is complete getSplits() will return split IDs that point to
        // the new storage location (table.getReadStorage()) but must still support old split IDs from the old
        // storage location for a while.
        if (!table.getReadStorage().contains(splitRange.getStart())) {
            for (AstyanaxStorage storage : table.getWriteStorage()) {
                if (storage.contains(splitRange.getStart()) && storage.getReadsAllowed()) {
                    return storage;
                }
            }
        }
        return table.getReadStorage();
    }

    /**
     * Implementation of {@link RowGroupResultSetIterator} with implementations for reading from a delta table.
     */
    private class DeltaRowGroupResultSetIterator extends RowGroupResultSetIterator {
        private final DeltaPlacement _placement;
        private final ConsistencyLevel _consistency;

        private DeltaRowGroupResultSetIterator(ResultSet resultSet, int prefetchLimit,
                                               DeltaPlacement placement, ConsistencyLevel consistency) {
            super(resultSet, prefetchLimit);
            _placement = placement;
            _consistency = consistency;
        }

        private DeltaRowGroupResultSetIterator(ResultSetFuture resultSetFuture, int prefetchLimit,
                                               DeltaPlacement placement, ConsistencyLevel consistency) {
            super(resultSetFuture, prefetchLimit);
            _placement = placement;
            _consistency = consistency;
        }

        @Override
        protected Object getKeyForRow(Row row) {
            // Key is always the first column, which is the delta row key
            return row.getBytesUnsafe(0);
        }

        @Override
        protected ResultSet queryRowGroupRowsAfter(Row row) {
            Range<RangeTimeUUID> columnRange = Range.greaterThan(new RangeTimeUUID(row.getUUID(1)));
            return columnScan(_placement, _placement.getDeltaTableDDL(), row.getBytesUnsafe(0), columnRange, true, Integer.MAX_VALUE, _consistency);
        }
    }

    /**
     * Reads columns from the delta, audit, or delta history table.  The range of columns, order, and limit can be
     * parameterized.
     */
    private ResultSet columnScan(DeltaPlacement placement, TableDDL tableDDL, ByteBuffer rowKey, Range<RangeTimeUUID> columnRange,
                                 boolean ascending, int limit, ConsistencyLevel consistency) {

        Select.Where where = QueryBuilder.select().column(tableDDL.getRowKeyColumnName())
                .column(tableDDL.getChangeIdColumnName()).column(tableDDL.getValueColumnName())
                .from(tableDDL.getTableMetadata())
                .where(eq(tableDDL.getRowKeyColumnName(), rowKey));

        if (columnRange.hasLowerBound()) {
            if (columnRange.lowerBoundType() == BoundType.CLOSED) {
                where = where.and(gte(tableDDL.getChangeIdColumnName(), columnRange.lowerEndpoint().getUuid()));
            } else {
                where = where.and(gt(tableDDL.getChangeIdColumnName(), columnRange.lowerEndpoint().getUuid()));
            }
        }

        if (columnRange.hasUpperBound()) {
            if (columnRange.upperBoundType() == BoundType.CLOSED) {
                where = where.and(lte(tableDDL.getChangeIdColumnName(), columnRange.upperEndpoint().getUuid()));
            } else {
                where = where.and(lt(tableDDL.getChangeIdColumnName(), columnRange.upperEndpoint().getUuid()));
            }
        }

        Statement statement = where
                .orderBy(ascending ? asc(tableDDL.getChangeIdColumnName()) : desc(tableDDL.getChangeIdColumnName()))
                .limit(limit)
                .setFetchSize(_singleRowFetchSize)
                .setConsistencyLevel(consistency);

        return placement.getKeyspace().getCqlSession().execute(statement);
    }

    @Override
    public Iterator<Change> readTimeline(Key key, boolean includeContentData, boolean includeAuditInformation, UUID start, UUID end,
                                         boolean reversed, long limit, ReadConsistency readConsistency) {
        if (_alwaysDelegateToAstyanax) {
            return _astyanaxReaderDAO.readTimeline(key, includeContentData, includeAuditInformation, start, end, reversed, limit, readConsistency);
        }

        checkNotNull(key, "key");
        checkArgument(limit > 0, "Limit must be >0");
        checkNotNull(readConsistency, "consistency");

        // Even though the API allows for a long limit CQL only supports integer values.  Anything longer than MAX_INT
        // is impractical given that a single Cassandra record must practically hold less than 2G rows since a wide row
        // cannot be larger than 2G bytes.

        int scaledLimit = (int) Math.min(Integer.MAX_VALUE, limit);

        AstyanaxTable table = (AstyanaxTable) key.getTable();
        AstyanaxStorage storage = table.getReadStorage();
        DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();
        ByteBuffer rowKey = storage.getRowKey(key.getKey());

        Range<RangeTimeUUID> columnRange = toRange(start, end, reversed);
        ConsistencyLevel consistency = SorConsistencies.toCql(readConsistency);

        // Read Delta and Compaction objects
        Iterator<Change> deltas = Iterators.emptyIterator();
        if (includeContentData) {
            TableDDL deltaDDL = placement.getDeltaTableDDL();
            deltas = decodeColumns(columnScan(placement, deltaDDL, rowKey, columnRange, !reversed, scaledLimit, consistency).iterator());
        }

        // Read Audit objects
        Iterator<Change> audits = Iterators.emptyIterator();
        Iterator<Change> deltaHistory = Iterators.emptyIterator();
        if (includeAuditInformation) {
            TableDDL auditDDL = placement.getAuditTableDDL();
            audits = decodeColumns(columnScan(placement, auditDDL, rowKey, columnRange, !reversed, scaledLimit, consistency).iterator());
            TableDDL deltaHistoryDDL = placement.getDeltaHistoryTableDDL();
            deltaHistory = decodeColumns(columnScan(placement, deltaHistoryDDL, rowKey, columnRange, !reversed, scaledLimit, consistency).iterator());
        }

        Iterator<Change> deltaPlusAudit = MergeIterator.merge(deltas, audits, reversed);
        return touch(MergeIterator.merge(deltaPlusAudit, deltaHistory, reversed));
    }

    @Override
    public Iterator<Change> getExistingAudits(Key key, UUID start, UUID end, ReadConsistency readConsistency) {
        if (_alwaysDelegateToAstyanax) {
            return _astyanaxReaderDAO.getExistingAudits(key, start, end, readConsistency);
        }

        AstyanaxTable table = (AstyanaxTable) key.getTable();
        AstyanaxStorage storage = table.getReadStorage();
        ByteBuffer rowKey = storage.getRowKey(key.getKey());
        DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();
        Range<RangeTimeUUID> columnRange = toRange(start, end, true);
        ConsistencyLevel consistency = SorConsistencies.toCql(readConsistency);
        TableDDL deltaHistoryDDL = placement.getDeltaHistoryTableDDL();
        return decodeColumns(columnScan(placement, deltaHistoryDDL, rowKey, columnRange, false, Integer.MAX_VALUE, consistency).iterator());
    }

    /**
     * Transforms the provided Row iterator into a {@link Change} iterator.
     */
    private Iterator<Change> decodeColumns(Iterator<Row> iter) {
        return Iterators.transform(iter,
                new Function<Row, Change>() {
                    @Override
                    public Change apply(Row row) {
                        return _changeEncoder.decodeChange(row.getUUID(1), row.getBytesUnsafe(2));
                    }
                });
    }

    /**
     * Converts a TimeUUID set of endpoints into a {@link Range}. of {@link RangeTimeUUID}s.  Both end points
     * are considered closed; that is, they are included in the range.
     */
    private Range<RangeTimeUUID> toRange(@Nullable UUID start, @Nullable UUID end, boolean reversed) {
        // If the range is reversed then start and end will also be reversed and must therefore be swapped.
        if (reversed) {
            UUID tmp = start;
            start = end;
            end = tmp;
        }

        if (start == null) {
            if (end == null) {
                return Range.all();
            } else {
                return Range.atMost(new RangeTimeUUID(end));
            }
        } else if (end == null) {
            return Range.atLeast(new RangeTimeUUID(start));
        }
        return Range.closed(new RangeTimeUUID(start), new RangeTimeUUID(end));
    }

    /**
     * {@link Range} needs comparable type.  This class thinly encapsulates a UUID and sorts as a TimeUUID.
     */
    private static class RangeTimeUUID implements Comparable<RangeTimeUUID>{
        private final UUID _uuid;

        private RangeTimeUUID(UUID uuid) {
            _uuid = uuid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RangeTimeUUID)) {
                return false;
            }
            return _uuid.equals(((RangeTimeUUID)o)._uuid);
        }

        @Override
        public int hashCode() {
            return _uuid.hashCode();
        }

        @Override
        public int compareTo(RangeTimeUUID o) {
            return TimeUUIDs.compare(_uuid, o._uuid);
        }

        private UUID getUuid() {
            return _uuid;
        }
    }

    /**
     * Helper method to return a record with no rows.
     */
    private Record emptyRecord(Key key) {
        return new RecordImpl(key,
                Iterators.<Map.Entry<UUID, Compaction>>emptyIterator(),
                Iterators.<Map.Entry<UUID, Change>>emptyIterator(),
                Iterators.<RecordEntryRawMetadata>emptyIterator());
    }

    // The following methods rely on using the Cassandra thrift call <code>describe_splits_ex()</code> to split
    // a token range into portions of approximately equal size.  There is currently no equivalent client-side
    // support for this call using CQL.  Therefore they must always defer to the Asytanax implementation.

    @Override
    public List<String> getSplits(Table table, int desiredRecordsPerSplit) {
        return _astyanaxReaderDAO.getSplits(table, desiredRecordsPerSplit);
    }

    @Override
    public ScanRangeSplits getScanRangeSplits(String placement, int desiredRecordsPerSplit, Optional<ScanRange> subrange) {
        return _astyanaxReaderDAO.getScanRangeSplits(placement, desiredRecordsPerSplit, subrange);
    }

    @Override
    public long count(Table table, ReadConsistency consistency) {
        return _astyanaxReaderDAO.count(table, consistency);
    }

    @Override
    public long count(Table table, @Nullable Integer limit, ReadConsistency consistency) {
        return _astyanaxReaderDAO.count(table, limit, consistency);
    }
}
