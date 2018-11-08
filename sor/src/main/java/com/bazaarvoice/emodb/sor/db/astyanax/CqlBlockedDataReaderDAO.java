package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.common.cassandra.CqlDriverConfiguration;
import com.bazaarvoice.emodb.common.cassandra.cqldriver.AdaptiveResultSet;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.db.*;
import com.bazaarvoice.emodb.sor.db.cql.*;
import com.bazaarvoice.emodb.table.db.DroppedTableException;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxTable;
import com.bazaarvoice.emodb.table.db.astyanax.Placement;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.Timed;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.MoreFutures;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.*;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.util.ByteBufferRangeImpl;
import java.util.concurrent.TimeoutException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.*;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

// Delegates to AstyanaxReaderDAO for non-CQL stuff
// Once we transition fully, we will stop delegating to Astyanax
public class CqlBlockedDataReaderDAO implements DataReaderDAO {

    private final Logger _log = LoggerFactory.getLogger(CqlBlockedDataReaderDAO.class);

    /**
     * Depending on the placement and type of data being queried (delta or delta history) the names of the
     * columns being queried can change.  However, by quering the columns in a fixed well-known order in each
     * {@link QueryBuilder#select()} the results can be efficiently read by position rather than name.
     */
    private static final int ROW_KEY_RESULT_SET_COLUMN = 0;
    private static final int CHANGE_ID_RESULT_SET_COLUMN = 1;
    private static final int VALUE_RESULT_SET_COLUMN = 2;
    private static final int BLOCK_RESULT_SET_COLUMN = 3;

    private final DataReaderDAO _astyanaxReaderDAO;
    private final ChangeEncoder _changeEncoder;
    private final PlacementCache _placementCache;
    private final CqlDriverConfiguration _driverConfig;
    private final Meter _randomReadMeter;
    private final Timer _readBatchTimer;
    private final DAOUtils _daoUtils;
    private final int _deltaPrefixLength;

    // Support AB testing of various uses of the CQL driver versus the older but (at this point) more vetted Astyanax driver.
    private volatile Supplier<Boolean> _useCqlForMultiGets = Suppliers.ofInstance(true);
    private volatile Supplier<Boolean> _useCqlForScans = Suppliers.ofInstance(true);

    @Inject
    public CqlBlockedDataReaderDAO(@CqlReaderDAODelegate DataReaderDAO delegate, PlacementCache placementCache,
                                   CqlDriverConfiguration driverConfig, ChangeEncoder changeEncoder,
                                   MetricRegistry metricRegistry, DAOUtils daoUtils, @PrefixLength int deltaPrefixLength) {
        _astyanaxReaderDAO = checkNotNull(delegate, "delegate");
        _placementCache = placementCache;
        _driverConfig = driverConfig;
        _changeEncoder = changeEncoder;
        _randomReadMeter = metricRegistry.meter(getMetricName("random-reads"));
        _readBatchTimer = metricRegistry.timer(getMetricName("readBatch"));
        _deltaPrefixLength = deltaPrefixLength;
        _daoUtils = daoUtils;

    }

    private String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.sor", "CqlDataReaderDAO", name);
    }

    // Since AB testing of CQL driver is temporary until proven out don't change the constructor to support this feature.
    // Inject the AB testing flags independently.  This will make backing these settings out easier in the future.

    @Inject
    public void setUseCqlforMultiGets(@CqlForMultiGets Supplier<Boolean> useCqlForMultiGets) {
        _useCqlForMultiGets = checkNotNull(useCqlForMultiGets, "useCqlForMultiGets");
    }

    @Inject
    public void setUseCqlforScans(@CqlForScans Supplier<Boolean> useCqlForScans) {
        _useCqlForScans = checkNotNull(useCqlForScans, "useCqlForScans");
    }

    /**
     * This CQL based read method works for a row with 64 deltas of 3 MB each. The same read with the AstyanaxDataReaderDAO
     * would give Thrift frame errors.
     */
    @Override
    public Record read(Key key, ReadConsistency consistency) {
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
        if (!_useCqlForMultiGets.get()) {
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
                entry -> readBatch(entry.getKey(), entry.getValue(), consistency))));
    }

    @Override
    public String getPlacementCluster(String placementName) {
        checkNotNull(placementName, "placement");

        DeltaPlacement placement = (DeltaPlacement) _placementCache.get(placementName);
        return placement.getKeyspace().getClusterName();
    }

    private Record read(Key key, ByteBuffer rowKey, ReadConsistency consistency, DeltaPlacement placement) {
        checkNotNull(key, "key");
        checkNotNull(consistency, "consistency");

        BlockedDeltaTableDDL tableDDL = placement.getBlockedDeltaTableDDL();

        Statement statement = selectDeltaFrom(tableDDL)
                .where(eq(tableDDL.getRowKeyColumnName(), rowKey))
                .setConsistencyLevel(SorConsistencies.toCql(consistency));


        // Track metrics
        _randomReadMeter.mark();

        Iterator<Iterable<Row>> groupedRows = deltaQuery(placement, statement, true, "Failed to read record %s", key);

        Iterable<Row> rows;
        if (groupedRows.hasNext()) {
            rows = groupedRows.next();
        } else {
            rows = ImmutableList.of();
        }

        // Convert the results into a Record object, lazily fetching the rest of the columns as necessary.
        return newRecordFromCql(key, rows, placement);
    }

    /**
     * Synchronously executes the provided statement.  The statement must query the delta table as returned from
     * {@link com.bazaarvoice.emodb.sor.db.astyanax.DeltaPlacement#getDeltaTableDDL()}
     */
    private Iterator<Iterable<Row>> deltaQuery(DeltaPlacement placement, Statement statement, boolean singleRow,
                                               String errorContext, Object... errorContextArgs) {
        return doDeltaQuery(placement, statement, singleRow, false, errorContext, errorContextArgs);
    }

    /**
     * Asynchronously executes the provided statement.  Although the iterator is returned immediately the actual results
     * may still be loading in the background.  The statement must query the delta table as returned from
     * {@link com.bazaarvoice.emodb.sor.db.astyanax.DeltaPlacement#getDeltaTableDDL()}
     */
    private Iterator<Iterable<Row>> deltaQueryAsync(DeltaPlacement placement, Statement statement, boolean singleRow,
                                                    String errorContext, Object... errorContextArgs) {
        return doDeltaQuery(placement, statement, singleRow, true, errorContext, errorContextArgs);
    }

    private Iterator<Iterable<Row>> doDeltaQuery(DeltaPlacement placement, Statement statement, boolean singleRow, boolean async,
                                                 String errorContext, Object... errorContextArgs) {
        // Set the fetch size and prefetch limits depending on whether the query is for a single row or multiple rows.
        int fetchSize = singleRow ? _driverConfig.getSingleRowFetchSize() : _driverConfig.getMultiRowFetchSize();
        int prefetchLimit = singleRow ? _driverConfig.getSingleRowPrefetchLimit() : _driverConfig.getMultiRowPrefetchLimit();

        Session session = placement.getKeyspace().getCqlSession();
        DeltaRowGroupResultSetIterator deltaRowGroupResultSetIterator;

        if (async) {
            ListenableFuture<ResultSet> resultSetFuture = AdaptiveResultSet.executeAdaptiveQueryAsync(session, statement, fetchSize);

            deltaRowGroupResultSetIterator = new DeltaRowGroupResultSetIterator(
                    resultSetFuture, prefetchLimit, placement, statement.getConsistencyLevel());

            Futures.addCallback(resultSetFuture, new MoreFutures.FailureCallback<ResultSet>() {
                @Override
                public void onFailure(Throwable t) {
                    _log.error(String.format(errorContext, errorContextArgs), t);
                }
            });
        } else {
            try {
                ResultSet resultSet = AdaptiveResultSet.executeAdaptiveQuery(session, statement, fetchSize);
                deltaRowGroupResultSetIterator = new DeltaRowGroupResultSetIterator(
                        resultSet, prefetchLimit, placement, statement.getConsistencyLevel());
            } catch (Throwable t) {
                _log.error(String.format(errorContext, errorContextArgs), t);
                throw t;
            }
        }

        return new CachingRowGroupIterator(deltaRowGroupResultSetIterator, _driverConfig.getRecordCacheSize(), _driverConfig.getRecordSoftCacheSize());
    }
    /**
     * Creates a Record instance for a given key and list of rows.  All rows must be from the same Cassandra row;
     * in other words, it is expected that row.getBytesUnsafe(ROW_KEY_RESULT_SET_COLUMN) returns the same value for
     * each row in rows.
     */
    private Record newRecordFromCql(Key key, Iterable<Row> rows, Placement placement) {
        Session session = placement.getKeyspace().getCqlSession();
        ProtocolVersion protocolVersion = session.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
        CodecRegistry codecRegistry = session.getCluster().getConfiguration().getCodecRegistry();

        Iterator<Map.Entry<UUID, Change>> changeIter = decodeChangesFromCql(new CqlDeltaIterator(rows.iterator(), BLOCK_RESULT_SET_COLUMN, CHANGE_ID_RESULT_SET_COLUMN, VALUE_RESULT_SET_COLUMN, false, _deltaPrefixLength, protocolVersion, codecRegistry));
        Iterator<Map.Entry<UUID, Compaction>> compactionIter = decodeCompactionsFromCql(new CqlDeltaIterator(rows.iterator(), BLOCK_RESULT_SET_COLUMN, CHANGE_ID_RESULT_SET_COLUMN, VALUE_RESULT_SET_COLUMN, false, _deltaPrefixLength, protocolVersion, codecRegistry));
        Iterator<RecordEntryRawMetadata> rawMetadataIter = rawMetadataFromCql(new CqlDeltaIterator(rows.iterator(), BLOCK_RESULT_SET_COLUMN, CHANGE_ID_RESULT_SET_COLUMN, VALUE_RESULT_SET_COLUMN, false, _deltaPrefixLength, protocolVersion, codecRegistry));

        return new RecordImpl(key, compactionIter, changeIter, rawMetadataIter);
    }

    /**
     * Converts a list of rows into Change instances.
     */
    private Iterator<Map.Entry<UUID, Change>> decodeChangesFromCql(final Iterator<Row> iter) {
        return Iterators.transform(iter, row ->
                Maps.immutableEntry(getChangeId(row), _changeEncoder.decodeChange(getChangeId(row), _daoUtils.skipPrefix(getValue(row)))));
    }

    /**
     * Like {@link #decodeChangesFromCql(java.util.Iterator)} except filtered to only include compactions.
     */
    private Iterator<Map.Entry<UUID, Compaction>> decodeCompactionsFromCql(final Iterator<Row> iter) {
        return new AbstractIterator<Map.Entry<UUID, Compaction>>() {
            @Override
            protected Map.Entry<UUID, Compaction> computeNext() {
                while (iter.hasNext()) {
                    Row row = iter.next();
                    Compaction compaction = _changeEncoder.decodeCompaction(_daoUtils.skipPrefix(getValue(row)));
                    if (compaction != null) {
                        return Maps.immutableEntry(getChangeId(row), compaction);
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
        return Iterators.transform(iter, row -> new RecordEntryRawMetadata()
                .withTimestamp(TimeUUIDs.getTimeMillis(getChangeId(row)))
                .withSize(_daoUtils.skipPrefix(getValue(row)).remaining()));
    }

    /**
     * Read a batch of keys that all belong to the same placement (ColumnFamily).
     */
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
        Collections.sort(rowKeys, Ordering.natural().onResultOf(entry -> entry.getKey()));

        // Group them into batches.  Cassandra may have to seek each row so prefer smaller batches.
        List<List<Map.Entry<ByteBuffer, Key>>> batches = Lists.partition(rowKeys, _driverConfig.getMaxRandomRowsBatchSize());

        // This algorithm is arranged such that rows are return in pages with size _fetchSize.  The rows are grouped
        // into row groups by common row key.  The first RECORD_CACHE_SIZE rows are cached for the row group
        // and any remaining rows are cached using soft references.  This places an upper bound on the memory
        // requirements needed while iterating.  If at any time a soft reference is lost C* is re-queried to
        // fetch the missing columns.

        return Iterators.concat(Iterators.transform(batches.iterator(),
                rowKeySubset -> {
                    Timer.Context timerCtx = _readBatchTimer.time();
                    try {
                        return rowQuery(rowKeySubset, consistency, placement);
                    } finally {
                        timerCtx.stop();
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

        BlockedDeltaTableDDL tableDDL = placement.getBlockedDeltaTableDDL();

        Statement statement = selectDeltaFrom(tableDDL)
                .where(in(tableDDL.getRowKeyColumnName(), keys))
                .setConsistencyLevel(SorConsistencies.toCql(consistency));

        Iterator<Iterable<Row>> rowGroups = deltaQueryAsync(placement, statement, false, "Failed to read records %s", rawKeyMap.values());

        return Iterators.concat(
                // First iterator reads the row groups found and transforms them to Records
                Iterators.transform(rowGroups, rows -> {
                    ByteBuffer keyBytes = getRawKeyFromRowGroup(rows);
                    Key key = rawKeyMap.remove(keyBytes);
                    assert key != null : "Query returned row with a key out of bound";
                    return newRecordFromCql(key, rows, placement);
                }),
                // Second iterator returns an empty Record for each key queried but not found.
                new AbstractIterator<Record>() {
                    private Iterator<Key> _nonExistentKeyIterator;

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
     * Returns a select statement builder for a {@link TableDDL} with the columns ordered in the order set by
     * {@link #ROW_KEY_RESULT_SET_COLUMN}, {@link #CHANGE_ID_RESULT_SET_COLUMN}, and {@link #VALUE_RESULT_SET_COLUMN}.
     */
    private Select selectFrom(TableDDL tableDDL) {
        return QueryBuilder.select()
                .column(tableDDL.getRowKeyColumnName())     // ROW_KEY_RESULT_SET_COLUMN
                .column(tableDDL.getChangeIdColumnName())   // CHANGE_ID_RESULT_SET_COLUMN
                .column(tableDDL.getValueColumnName())      // VALUE_RESULT_SET_COLUMN
                .from(tableDDL.getTableMetadata());
    }

    private Select selectDeltaFrom(BlockedDeltaTableDDL tableDDL) {
        return QueryBuilder.select()
                .column(tableDDL.getRowKeyColumnName())     // ROW_KEY_RESULT_SET_COLUMN
                .column(tableDDL.getChangeIdColumnName())   // CHANGE_ID_RESULT_SET_COLUMN
                .column(tableDDL.getValueColumnName())      // VALUE_RESULT_SET_COLUMN
                .column(tableDDL.getBlockColumnName())      // BLOCK_ID_RESULT_SET COLUMN
                .from(tableDDL.getTableMetadata());
    }

    private ByteBuffer getKey(Row row) {
        return row.getBytesUnsafe(ROW_KEY_RESULT_SET_COLUMN);
    }

    private UUID getChangeId(Row row) {
        return row.getUUID(CHANGE_ID_RESULT_SET_COLUMN);
    }

    private int getBlock(Row row) {
        return row.getInt(BLOCK_RESULT_SET_COLUMN);
    }

    private ByteBuffer getValue(Row row) {
        return row.getBytesUnsafe(VALUE_RESULT_SET_COLUMN);
    }

    /**
     * A few notes on this method:
     * <ol>
     * <li>All rows in the row group have the same key, so choosing the first row is safe.</li>
     * <li>The rowGroup will always contain at least one row.</li>
     * <li>The row group has at least the first row in hard cache, so iterating to the first row will never
     * result in a new CQL query.</li>
     * </ol>
     */
    private ByteBuffer getRawKeyFromRowGroup(Iterable<Row> rowGroup) {
        Iterator<Row> iter = rowGroup.iterator();
        // Sanity check
        assert iter.hasNext() : "Row group should never contain zero rows";
        return getKey(iter.next());
    }

    /*
      Its similar to getRawKeyFromRowGroup but should be used where the rowGroup can have no rows as well.
    */
    private ByteBuffer getRawKeyFromRowGroupOrNull(Iterable<Row> filteredRowGroup) {
        Iterator<Row> iter = filteredRowGroup.iterator();
        return iter.hasNext() ? getKey(iter.next()) : null;
    }

    private <T> Iterator<T> touch(Iterator<T> iter) {
        // Could return a Guava PeekingIterator after "if (iter.hasNext()) iter.peek()", but simply calling hasNext()
        // is sufficient for the iterator implementations used by this DAO class...
        iter.hasNext();
        return iter;
    }

    @Timed(name = "bv.emodb.sor.CqlDataReaderDAO.scan", absolute = true)
    @Override
    public Iterator<Record> scan(Table tbl, @Nullable String fromKeyExclusive, final LimitCounter ignore_limit,
                                 final ReadConsistency consistency) {
        // Note:  The LimitCounter is passed in as an artifact of Astyanax batching and was used as a mechanism to
        // control paging.  The CQL driver natively performs this functionality so it is not used here.  The caller
        // will apply limit boundaries on the results from this method.
        if (!_useCqlForScans.get()) {
            return _astyanaxReaderDAO.scan(tbl, fromKeyExclusive, ignore_limit, consistency);
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
        if (!_useCqlForScans.get()) {
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
    private Iterator<Iterable<Row>> rowScan(DeltaPlacement placement, @Nullable AstyanaxTable table, ByteBufferRange keyRange,
                                            ReadConsistency consistency) {
        ByteBuffer startToken = keyRange.getStart();
        ByteBuffer endToken = keyRange.getEnd();

        // Note: if Cassandra is asked to perform a token range query where start >= end it will wrap
        // around which is absolutely *not* what we want.
        checkArgument(AstyanaxStorage.compareKeys(startToken, endToken) < 0, "Cannot scan rows which loop from maximum- to minimum-token");

        BlockedDeltaTableDDL tableDDL = placement.getBlockedDeltaTableDDL();

        Statement statement = selectDeltaFrom(tableDDL)
                .where(gt(token(tableDDL.getRowKeyColumnName()), startToken))
                .and(lte(token(tableDDL.getRowKeyColumnName()), endToken))
                .setConsistencyLevel(SorConsistencies.toCql(consistency));

        return deltaQueryAsync(placement, statement, false, "Failed to scan token range [%s, %s] for %s",
                ByteBufferUtil.bytesToHex(startToken), ByteBufferUtil.bytesToHex(endToken),
                table != null ? table : "multiple tables");
    }

    /**
     * Similar to {@link #rowScan(DeltaPlacement, AstyanaxTable, com.netflix.astyanax.model.ByteBufferRange, com.bazaarvoice.emodb.sor.api.ReadConsistency)}
     * except this method converts each C* row into a Record.
     */
    private Iterator<Record> recordScan(DeltaPlacement placement, AstyanaxTable table, ByteBufferRange keyRange,
                                        ReadConsistency consistency) {

        Iterator<Iterable<Row>> rowGroups = rowScan(placement, table, keyRange, consistency);
        return decodeRows(rowGroups, table, placement);
    }

    /**
     * Converts rows from a single C* row to a Record.
     */
    private Iterator<Record> decodeRows(Iterator<Iterable<Row>> rowGroups, final AstyanaxTable table, Placement placement) {
        return Iterators.transform(rowGroups, rowGroup -> {
            String key = AstyanaxStorage.getContentKey(getRawKeyFromRowGroup(rowGroup));
            return newRecordFromCql(new Key(table, key), rowGroup, placement);
        });
    }

    @Override
    public Iterator<MultiTableScanResult> multiTableScan(final MultiTableScanOptions query, final TableSet tables,
                                                         final LimitCounter limit, final ReadConsistency consistency, @Nullable Instant cutoffTime) {
        if (!_useCqlForScans.get()) {
            return _astyanaxReaderDAO.multiTableScan(query, tables, limit, consistency, cutoffTime);
        }

        checkNotNull(query, "query");
        String placementName = checkNotNull(query.getPlacement(), "placement");
        final DeltaPlacement placement = (DeltaPlacement) _placementCache.get(placementName);

        ScanRange scanRange = Objects.firstNonNull(query.getScanRange(), ScanRange.all());

        // Since the range may wrap from high to low end of the token range we need to unwrap it
        List<ScanRange> ranges = scanRange.unwrapped();

        return touch(FluentIterable.from(ranges)
                .transformAndConcat(rowRange -> scanMultiTableRows(
                        tables, placement, rowRange.asByteBufferRange(), limit, query.isIncludeDeletedTables(),
                        query.isIncludeMirrorTables(), consistency, cutoffTime))
                .iterator());
    }

    /**
     * Decodes rows returned by scanning across tables.
     */
    private Iterable<MultiTableScanResult> scanMultiTableRows(
            final TableSet tables, final DeltaPlacement placement, final ByteBufferRange rowRange,
            final LimitCounter limit, final boolean includeDroppedTables, final boolean includeMirrorTables,
            final ReadConsistency consistency, final Instant cutoffTime) {

        // Avoiding pinning multiple decoded rows into memory at once.
        return () -> limit.limit(new AbstractIterator<MultiTableScanResult>() {
            private PeekingIterator<Iterable<Row>> _iter = Iterators.peekingIterator(
                    rowScan(placement, null, rowRange, consistency));

            private long _lastTableUuid = -1;
            private AstyanaxTable _table = null;
            private boolean _droppedTable;
            private boolean _primaryTable;

            @Override
            protected MultiTableScanResult computeNext() {
                while (_iter.hasNext()) {
                    // Get the next rows from the grouping iterator.  All rows in the returned Iterable
                    // are from the same Cassandra wide row (in other words, they share the same key).
                    final Iterable<Row> rows = _iter.next();

                    // filter the rows if a cutOff time is specified.
                    Iterable<Row> filteredRows = rows;
                    if (cutoffTime != null) {
                        filteredRows = getFilteredRows(rows, cutoffTime);
                    }

                    // Convert the filteredRows into a Record object
                    ByteBuffer rowKey = getRawKeyFromRowGroupOrNull(filteredRows);
                    // rowKey can be null if "all" the rows of the cassandra record are after the cutoff time. In such case ignore that record and continue.
                    if (rowKey == null) {
                        continue;
                    }

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
                    if ((!includeDroppedTables && _droppedTable) || (!includeMirrorTables && !_primaryTable)) {
                        _iter = skipToNextTable(tableUuid);
                        continue;
                    }

                    int shardId = AstyanaxStorage.getShardId(rowKey);
                    String key = AstyanaxStorage.getContentKey(rowKey);
                    Record record = newRecordFromCql(new Key(_table, key), filteredRows, placement);
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
                        return Iterators.peekingIterator(rowScan(placement, null, updatedRange, consistency));
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

        private DeltaRowGroupResultSetIterator(ListenableFuture<ResultSet> resultSetFuture, int prefetchLimit,
                                               DeltaPlacement placement, ConsistencyLevel consistency) {
            super(resultSetFuture, prefetchLimit);
            _placement = placement;
            _consistency = consistency;
        }

        @Override
        protected Object getKeyForRow(Row row) {
            return CqlBlockedDataReaderDAO.this.getKey(row);
        }

        @Override
        protected ResultSet queryRowGroupRowsAfter(Row row) {
            Range<RangeTimeUUID> columnRange = Range.greaterThan(new RangeTimeUUID(getChangeId(row)));
            return columnScan(_placement, _placement.getDeltaTableDDL(), getKey(row),
                    columnRange, true, _consistency);
        }
    }

    /**
     * Reads columns from the delta or delta history table.  The range of columns, order, and limit can be
     * parameterized.
     */
    private ResultSet columnScan(DeltaPlacement placement, TableDDL tableDDL, ByteBuffer rowKey, Range<RangeTimeUUID> columnRange,
                                 boolean ascending, ConsistencyLevel consistency) {

        Select.Where where = (tableDDL == placement.getDeltaTableDDL() ? selectDeltaFrom(placement.getBlockedDeltaTableDDL()) : selectFrom(tableDDL))
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
                .setConsistencyLevel(consistency);

        return AdaptiveResultSet.executeAdaptiveQuery(placement.getKeyspace().getCqlSession(), statement, _driverConfig.getSingleRowFetchSize());
    }

    @Override
    public Iterator<Change> readTimeline(Key key, boolean includeContentData, UUID start, UUID end,
                                         boolean reversed, long limit, ReadConsistency readConsistency) {
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
            ProtocolVersion protocolVersion = placement.getKeyspace().getCqlSession().getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
            CodecRegistry codecRegistry = placement.getKeyspace().getCqlSession().getCluster().getConfiguration().getCodecRegistry();

            deltas = decodeDeltaColumns(Iterators.limit(new CqlDeltaIterator(columnScan(placement, deltaDDL, rowKey, columnRange, !reversed, consistency).iterator(), BLOCK_RESULT_SET_COLUMN, CHANGE_ID_RESULT_SET_COLUMN, VALUE_RESULT_SET_COLUMN, reversed, _deltaPrefixLength, protocolVersion, codecRegistry), scaledLimit));
        }

        // Read History objects
        Iterator<Change> deltaHistory = Iterators.emptyIterator();
        TableDDL deltaHistoryDDL = placement.getDeltaHistoryTableDDL();
        deltaHistory = decodeColumns(Iterators.limit(columnScan(placement, deltaHistoryDDL, rowKey, columnRange, !reversed, consistency).iterator(), scaledLimit));

        return touch(MergeIterator.merge(deltas, deltaHistory, reversed));
    }

    @Override
    public Iterator<Change> getExistingHistories(Key key, UUID start, UUID end, ReadConsistency readConsistency) {
        AstyanaxTable table = (AstyanaxTable) key.getTable();
        AstyanaxStorage storage = table.getReadStorage();
        ByteBuffer rowKey = storage.getRowKey(key.getKey());
        DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();
        Range<RangeTimeUUID> columnRange = toRange(start, end, true);
        ConsistencyLevel consistency = SorConsistencies.toCql(readConsistency);
        TableDDL deltaHistoryDDL = placement.getDeltaHistoryTableDDL();
        return decodeColumns(columnScan(placement, deltaHistoryDDL, rowKey, columnRange, false, consistency).iterator());
    }

    /**
     * Transforms the provided Row iterator into a {@link Change} iterator.
     */
    private Iterator<Change> decodeColumns(Iterator<Row> iter) {
        return Iterators.transform(iter, row -> _changeEncoder.decodeChange(getChangeId(row), getValue(row)));
    }

    private Iterator<Change> decodeDeltaColumns(Iterator<Row> iter) {
        return Iterators.transform(iter, row -> _changeEncoder.decodeChange(getChangeId(row), _daoUtils.skipPrefix(getValue(row))));
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
    private static class RangeTimeUUID implements Comparable<RangeTimeUUID> {
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
            return _uuid.equals(((RangeTimeUUID) o)._uuid);
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

    @VisibleForTesting
    public static Iterable<Row> getFilteredRows(Iterable<Row> rows, Instant cutoffTime) {
        if (cutoffTime == null) {
            return rows;
        }
        return () -> Iterators.filter(rows.iterator(), row -> (TimeUUIDs.getTimeMillis(row.getUUID(CHANGE_ID_RESULT_SET_COLUMN)) < cutoffTime.toEpochMilli()));
    }

    // The following methods rely on using the Cassandra thrift call <code>describe_splits_ex()</code> to split
    // a token range into portions of approximately equal size.  There is currently no equivalent client-side
    // support for this call using CQL.  Therefore they must always defer to the Asytanax implementation.

    @Override
    public List<String> getSplits(Table table, int desiredRecordsPerSplit, int splitQuerySize) throws TimeoutException {
        return _astyanaxReaderDAO.getSplits(table, desiredRecordsPerSplit, splitQuerySize);
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