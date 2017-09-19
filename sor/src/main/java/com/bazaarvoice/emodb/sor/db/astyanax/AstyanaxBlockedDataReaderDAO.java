package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.cassandra.astyanax.KeyspaceUtil;
import com.bazaarvoice.emodb.common.cassandra.nio.BufferUtils;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.AbstractBatchReader;
import com.bazaarvoice.emodb.sor.db.*;
import com.bazaarvoice.emodb.table.db.DroppedTableException;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxTable;
import com.bazaarvoice.emodb.table.db.astyanax.DataCopyDAO;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.annotation.Timed;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.inject.Inject;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.ConnectionContext;
import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.TokenRange;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.IsTimeoutException;
import com.netflix.astyanax.connectionpool.impl.TokenRangeImpl;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.CfSplit;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.shallows.EmptyKeyspaceTracerFactory;
import com.netflix.astyanax.thrift.AbstractKeyspaceOperationImpl;
import com.netflix.astyanax.util.ByteBufferRangeImpl;
import com.netflix.astyanax.util.RangeBuilder;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.EndpointDetails;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.transport.TTransportException;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cassandra implementation of {@link DataReaderDAO} that uses the Netflix Astyanax client library.
 */
public class AstyanaxBlockedDataReaderDAO implements DataReaderDAO, DataCopyDAO, AstyanaxKeyScanner {
    private static final int MAX_RANDOM_ROWS_BATCH = 50;
    private static final int MAX_SCAN_ROWS_BATCH = 250;
    private static final int SCAN_ROW_BATCH_INCREMENT = 50;
    private static final int MAX_COLUMNS_BATCH = 50;
    private static final int MAX_COLUMN_SCAN_BATCH = 250;

    private static final Token.TokenFactory _tokenFactory = new ByteOrderedPartitioner().getTokenFactory();
    private static final ByteBufferRange _maxColumnsRange = new RangeBuilder().setLimit(MAX_COLUMNS_BATCH).build();

    private final ChangeEncoder _changeEncoder;
    private final PlacementCache _placementCache;
    private final Timer _readBatchTimer;
    private final Timer _scanBatchTimer;
    private final Meter _randomReadMeter;
    private final Meter _scanReadMeter;
    private final Meter _largeRowReadMeter;
    private final Meter _copyMeter;
    private final DAOUtils _daoUtils;
    private final int _deltaPrefixLength;

    @Inject
    public AstyanaxBlockedDataReaderDAO(PlacementCache placementCache, ChangeEncoder changeEncoder, MetricRegistry metricRegistry,
                                 DAOUtils daoUtils, @PrefixLength int deltaPrefixLength) {
        checkArgument(deltaPrefixLength > 0, "delta prefix length must be > 0");

        _placementCache = placementCache;
        _changeEncoder = changeEncoder;
        _readBatchTimer = metricRegistry.timer(getMetricName("readBatch"));
        _scanBatchTimer = metricRegistry.timer(getMetricName("scanBatch"));
        _randomReadMeter = metricRegistry.meter(getMetricName("random-reads"));
        _scanReadMeter = metricRegistry.meter(getMetricName("scan-reads"));
        _largeRowReadMeter = metricRegistry.meter(getMetricName("large-row-reads"));
        _copyMeter = metricRegistry.meter(getMetricName("copy"));
        _daoUtils = daoUtils;
        _deltaPrefixLength = deltaPrefixLength;
    }

    private String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.sor", "AstyanaxDataReaderDAO", name);
    }

    @Timed (name = "bv.emodb.sor.AstyanaxDataReaderDAO.count", absolute = true)
    @Override
    public long count(Table table, ReadConsistency consistency) {
        return count(table, null, consistency);
    }

    @Timed (name = "bv.emodb.sor.AstyanaxDataReaderDAO.count", absolute = true)
    @Override
    public long count(Table tbl, @Nullable Integer limit, ReadConsistency consistency) {
        checkNotNull(tbl, "table");
        checkNotNull(consistency, "consistency");

        // The current implementation scans through every row in the table.  It's very expensive for large tables.
        // Given a limit, count up to the limit, and then estimate for the remaining range splits.

        AstyanaxTable table = (AstyanaxTable) tbl;
        AstyanaxStorage storage = table.getReadStorage();

        // Range query all the shards and count the number of rows in each.
        long count = 0;
        Iterator<String> it = scanKeys(storage, consistency);
        while (it.hasNext()) {
            String fromKey = it.next();
            count++;
            if (limit != null && count > limit) {
                // Clients may just want to distinguish "a few" vs. "lots.  Calculate an exact count up to 'limit'
                // then estimate anything larger by adding the estimated sizes for the remaining splits.
                count += approximateCount(table, consistency, fromKey);
                return count;
            }
        }

        return count;
    }

    private long approximateCount(Table tbl, ReadConsistency consistency, String fromKey) {
        checkNotNull(tbl, "table");
        checkNotNull(consistency, "consistency");

        long count = 0;
        List<CfSplit> cfSplits = getCfSplits(tbl, 10000, fromKey);
        for (CfSplit split : cfSplits) {
            count += split.getRowCount();
        }

        return count;
    }

    @Timed (name = "bv.emodb.sor.AstyanaxDataReaderDAO.read", absolute = true)
    @Override
    public Record read(Key key, ReadConsistency consistency) {
        checkNotNull(key, "key");
        checkNotNull(consistency, "consistency");

        AstyanaxTable table = (AstyanaxTable) key.getTable();
        AstyanaxStorage storage = table.getReadStorage();
        DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();
        ByteBuffer rowKey = storage.getRowKey(key.getKey());

        // Query for Delta & Compaction info, just the first 50 columns for now.
        ColumnList<DeltaKey> columns = execute(placement.getKeyspace()
                        .prepareQuery(placement.getBlockedDeltaColumnFamily(), SorConsistencies.toAstyanax(consistency))
                        .getKey(rowKey)
                        .withColumnRange(_maxColumnsRange),
                "read record at placement %s, table %s, key %s",
                placement.getName(), table.getName(), key.getKey());

        // Track metrics
        _randomReadMeter.mark();

        // Convert the results into a Record object, lazily fetching the rest of the columns as necessary.
        return newRecord(key, rowKey, columns, _maxColumnsRange.getLimit(), consistency, null);
    }

    @Timed (name = "bv.emodb.sor.AstyanaxDataReaderDAO.readAll", absolute = true)
    @Override
    public Iterator<Record> readAll(Collection<Key> keys, final ReadConsistency consistency) {
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

    /**
     * Read a batch of keys that all belong to the same placement (ColumnFamily).
     */
    private Iterator<Record> readBatch(final DeltaPlacement placement, Collection<Key> keys, final ReadConsistency consistency) {
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

        // Group them into batches.  Cassandra may have to seek to each row so prefer smaller batches.
        List<List<Map.Entry<ByteBuffer, Key>>> batches = Lists.partition(rowKeys, MAX_RANDOM_ROWS_BATCH);

        // This algorithm is arranged such that only one row of raw decoded changes is pinned in memory at a time.
        // If there are lots of rows with large #s of deltas our memory use should be bounded by the size of the
        // single row with the most/largest deltas + the largest raw thrift byte buffers for a single query.
        return Iterators.concat(Iterators.transform(batches.iterator(),
                new Function<List<Map.Entry<ByteBuffer, Key>>, Iterator<Record>>() {
                    @Override
                    public Iterator<Record> apply(List<Map.Entry<ByteBuffer, Key>> rowKeys) {
                        Timer.Context timerCtx = _readBatchTimer.time();
                        try {
                            return rowQuery(placement, rowKeys, consistency);
                        } finally {
                            timerCtx.stop();
                        }
                    }
                }));
    }

    @Override
    public Iterator<Change> readTimeline(Key key, boolean includeContentData, boolean includeAuditInformation,
                                         UUID start, UUID end, boolean reversed, long limit, ReadConsistency consistency) {
        checkNotNull(key, "key");
        checkArgument(limit > 0, "Limit must be >0");
        checkNotNull(consistency, "consistency");

        AstyanaxTable table = (AstyanaxTable) key.getTable();
        AstyanaxStorage storage = table.getReadStorage();
        DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();
        ByteBuffer rowKey = storage.getRowKey(key.getKey());

        // Read Delta and Compaction objects
        Iterator<Change> deltas = Iterators.emptyIterator();
        if (includeContentData) {
            ColumnFamily<ByteBuffer, DeltaKey> cf = placement.getBlockedDeltaColumnFamily();
            DeltaKey deltaStart = start != null ? new DeltaKey(start, 0) : null;
            DeltaKey deltaEnd = end != null ? new DeltaKey(end, Integer.MAX_VALUE) : null;
            deltas = decodeDeltaColumns(new LimitCounter(limit).limit(new AstyanaxDeltaIterator(columnScan(rowKey, placement, cf, deltaStart, deltaEnd, reversed, _deltaKeyInc, Long.MAX_VALUE, 0, consistency), reversed, _deltaPrefixLength)));

        }

        // Read Audit objects
        Iterator<Change> audits = Iterators.emptyIterator();
        Iterator<Change> deltaHistory = Iterators.emptyIterator();
        if (includeAuditInformation) {
            ColumnFamily<ByteBuffer, UUID> cf = placement.getAuditColumnFamily();
            audits = decodeColumns(columnScan(rowKey, placement, cf, start, end, reversed, _uuidInc, limit, 0, consistency));
            ColumnFamily<ByteBuffer, UUID> deltaHistoryCf = placement.getDeltaHistoryColumnFamily();
            deltaHistory = decodeColumns(columnScan(rowKey, placement, deltaHistoryCf, start, end, reversed, _uuidInc, limit, 0, consistency));
        }

        Iterator<Change> deltaPlusAudit = MergeIterator.merge(deltas, audits, reversed);
        return touch(MergeIterator.merge(deltaPlusAudit, deltaHistory, reversed));
    }

    @Override
    public Iterator<Change> getExistingAudits(Key key, UUID start, UUID end, ReadConsistency consistency) {
        AstyanaxTable table = (AstyanaxTable) key.getTable();
        AstyanaxStorage storage = table.getReadStorage();
        ByteBuffer rowKey = storage.getRowKey(key.getKey());
        DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();
        ColumnFamily<ByteBuffer, UUID> cf = placement.getDeltaHistoryColumnFamily();
        return decodeColumns(columnScan(rowKey, placement, cf, start, end, true, _uuidInc, MAX_COLUMN_SCAN_BATCH, 0, consistency));
    }

    @Timed (name = "bv.emodb.sor.AstyanaxDataReaderDAO.scan", absolute = true)
    @Override
    public Iterator<Record> scan(Table tbl, @Nullable String fromKeyExclusive, final LimitCounter limit, final ReadConsistency consistency) {
        checkNotNull(tbl, "table");
        checkNotNull(limit, "limit");
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
                    return decodeRows(
                            rowScan(placement, keyRange, _maxColumnsRange, limit, consistency),
                            table, _maxColumnsRange.getLimit(), consistency);
                }
                return endOfData();
            }
        }));
    }

    @Override
    public Iterator<String> scanKeys(AstyanaxStorage storage, final ReadConsistency consistency) {
        checkNotNull(storage, "storage");
        checkNotNull(consistency, "consistency");

        final DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();

        // We just want row keys, but get at least one column so we can ignore range ghosts.
        final ByteBufferRange columnRange = new RangeBuilder().setLimit(1).build();
        final LimitCounter unlimited = LimitCounter.max();

        // Loop over all the range prefixes (2^shardsLog2 of them) and, for each, execute Cassandra queries to
        // page through the records with that prefix.
        final Iterator<ByteBufferRange> scanIter = storage.scanIterator(null);
        return touch(Iterators.concat(new AbstractIterator<Iterator<String>>() {
            @Override
            protected Iterator<String> computeNext() {
                if (scanIter.hasNext()) {
                    ByteBufferRange keyRange = scanIter.next();
                    return decodeKeys(rowScan(placement, keyRange, columnRange, unlimited, consistency));
                }
                return endOfData();
            }
        }));
    }

    @Timed (name = "bv.emodb.sor.AstyanaxDataReaderDAO.getSplits", absolute = true)
    @Override
    public List<String> getSplits(Table tbl, int desiredRecordsPerSplit) {
        checkNotNull(tbl, "table");
        List<String> splits = Lists.newArrayList();
        List<CfSplit> cfSplits = getCfSplits(tbl, desiredRecordsPerSplit);
        for (CfSplit split : cfSplits) {
            ByteBuffer begin = parseTokenString(split.getStartToken());
            ByteBuffer finish = parseTokenString(split.getEndToken());

            splits.add(SplitFormat.encode(new ByteBufferRangeImpl(begin, finish, -1, false)));
        }
        // Randomize the splits so, if processed somewhat in parallel, requests distribute around the ring.
        Collections.shuffle(splits);
        return splits;
    }

    private List<CfSplit> getCfSplits(Table tbl, int desiredRecordsPerSplit) {
        return getCfSplits(tbl, desiredRecordsPerSplit, null);
    }

    private List<CfSplit> getCfSplits(Table tbl, int desiredRecordsPerSplit, @Nullable String fromKey) {
        checkNotNull(tbl, "table");

        AstyanaxTable table = (AstyanaxTable) tbl;
        AstyanaxStorage storage = table.getReadStorage();
        DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();
        Keyspace keyspace = placement.getKeyspace().getAstyanaxKeyspace();
        ColumnFamily<ByteBuffer, DeltaKey> cf = placement.getBlockedDeltaColumnFamily();

        // Create at least one split per shard, perhaps more if a shard is large.
        List<CfSplit> splits = Lists.newArrayList();
        Iterator<ByteBufferRange> it = storage.scanIterator(fromKey);
        Collection<TokenRange> allTokenRanges = describeCassandraTopology(keyspace).values();
        while (it.hasNext()) {
            ByteBufferRange keyRange = it.next();

            String start = toTokenString(keyRange.getStart());
            String end = toTokenString(keyRange.getEnd());

            splits.addAll(getCfSplits(keyspace, cf, start, end, desiredRecordsPerSplit, allTokenRanges));
        }
        return splits;
    }

    private List<CfSplit> getCfSplits(Keyspace keyspace, ColumnFamily<ByteBuffer, DeltaKey> cf, String start,
                                      String end, int desiredRecordsPerSplit, Iterable<TokenRange> allTokenRanges) {
        // There is a hole in the describeSplitsEx() call where if the call is routed to a Cassandra node which does
        // not have a replica of the requested token range then it will return a single split equivalent to the requested
        // range.  To accommodate this each query is routed to a host that is verified to have a replica of the range.

        ScanRange splitRange = ScanRange.create(parseTokenString(start), parseTokenString(end));
        List<CfSplit> cfSplits = Lists.newArrayList();

        // Iterate over the entire ring to find the token ranges which overlap with the provided range
        for (TokenRange hostTokenRange : allTokenRanges) {
            ScanRange hostSplitRange = ScanRange.create(
                    parseTokenString(hostTokenRange.getStartToken()),
                    parseTokenString(hostTokenRange.getEndToken()));

            // Use the intersection to determine if there is overlap
            for (ScanRange intersection : splitRange.intersection(hostSplitRange)) {
                // Try once on each host until splits are returned

                List<CfSplit> intersectionSplits = null;
                for (Iterator<String> hosts = hostTokenRange.getEndpoints().iterator(); hosts.hasNext() && intersectionSplits == null; ) {
                    String host = hosts.next();
                    try {
                        intersectionSplits = KeyspaceUtil.pin(keyspace).toHost(host)
                                .describeSplitsEx(cf.getName(), toTokenString(intersection.getFrom()),
                                        toTokenString(intersection.getTo()),
                                        desiredRecordsPerSplit, intersection.getFrom());
                    } catch (ConnectionException e) {
                        // If there is another host to try then do so, otherwise raise the exception
                        if (!hosts.hasNext()) {
                            throw Throwables.propagate(e);
                        }
                    }
                }

                assert intersectionSplits != null : "Exception would have been thrown if no host had responded successfully";
                cfSplits.addAll(intersectionSplits);
            }
        }

        return cfSplits;
    }

    @Timed (name = "bv.emodb.sor.AstyanaxDataReaderDAO.getSplit", absolute = true)
    @Override
    public Iterator<Record> getSplit(Table tbl, String split, @Nullable String fromKeyExclusive, LimitCounter limit, ReadConsistency consistency) {
        checkNotNull(tbl, "table");
        checkNotNull(split, "split");
        checkNotNull(limit, "limit");
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

        // In contrast to the scan() method, scan a single range prefix (the one associated with this split).
        return touch(decodeRows(
                rowScan(placement, keyRange, _maxColumnsRange, limit, consistency),
                table, _maxColumnsRange.getLimit(), consistency));
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
     * Gets the topology for a Cassandra keyspace as a Multimap, where the keys identify a rack (or availability zone
     * in Amazon) and the values are the token ranges for each host in that rack.  For example, for a well distributed
     * ring of 12 hosts and a replication factor of 3 this method would return a Multimap with 3 keys and each key would
     * contain 4 token ranges.
     */
    private Multimap<String, TokenRange> describeCassandraTopology(final Keyspace keyspace) {
        try {
            @SuppressWarnings ("unchecked")
            ConnectionPool<Cassandra.Client> connectionPool = (ConnectionPool<Cassandra.Client>) keyspace.getConnectionPool();

            return connectionPool.executeWithFailover(
                    new AbstractKeyspaceOperationImpl<Multimap<String, TokenRange>>(EmptyKeyspaceTracerFactory.getInstance().newTracer(CassandraOperationType.DESCRIBE_RING), keyspace.getKeyspaceName()) {
                        @Override
                        protected Multimap<String, TokenRange> internalExecute(Cassandra.Client client, ConnectionContext state)
                                throws Exception {
                            Multimap<String, TokenRange> racks = ArrayListMultimap.create();
                            for (org.apache.cassandra.thrift.TokenRange tokenRange : client.describe_local_ring(getKeyspace())) {
                                // The final local endpoint "owns" the token range, the rest are for replication
                                EndpointDetails endpointDetails = Iterables.getLast(tokenRange.getEndpoint_details());
                                racks.put(endpointDetails.getRack(),
                                        new TokenRangeImpl(tokenRange.getStart_token(), tokenRange.getEnd_token(), tokenRange.getEndpoints()));
                            }
                            return Multimaps.unmodifiableMultimap(racks);
                        }
                    },
                    keyspace.getConfig().getRetryPolicy().duplicate()).getResult();
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public ScanRangeSplits getScanRangeSplits(String placementName, int desiredRecordsPerSplit, Optional<ScanRange> subrange) {
        checkNotNull(placementName, "placement");
        checkArgument(desiredRecordsPerSplit >= 0, "Min records per split too low");

        DeltaPlacement placement = (DeltaPlacement) _placementCache.get(placementName);
        CassandraKeyspace keyspace = placement.getKeyspace();
        ColumnFamily<ByteBuffer, DeltaKey> cf = placement.getBlockedDeltaColumnFamily();

        // Get the topology so the splits can be grouped by rack
        Multimap<String, TokenRange> racks = describeCassandraTopology(keyspace.getAstyanaxKeyspace());
        Collection<TokenRange> allTokenRanges = racks.values();
        ScanRangeSplits.Builder builder = ScanRangeSplits.builder();

        for (Map.Entry<String, Collection<TokenRange>> entry : racks.asMap().entrySet()) {
            String rack = entry.getKey();

            Collection<TokenRange> tokenRanges = entry.getValue();
            for (TokenRange tokenRange : tokenRanges) {
                if (subrange.isPresent()) {
                    // Find the intersecting token ranges (if any) and add the splits for the intersection
                    ByteBuffer rangeStart = parseTokenString(tokenRange.getStartToken());
                    ByteBuffer rangeEnd = parseTokenString(tokenRange.getEndToken());

                    List<ScanRange> intersections = ScanRange.create(rangeStart, rangeEnd).intersection(subrange.get());
                    for (ScanRange scanRange : intersections) {
                        TokenRange intersectingTokenRange = new TokenRangeImpl(
                                toTokenString(scanRange.getFrom()), toTokenString(scanRange.getTo()), tokenRange.getEndpoints());

                        addScanRangeSplitsForTokenRange(keyspace, cf, rack, intersectingTokenRange,
                                desiredRecordsPerSplit, allTokenRanges, builder);
                    }
                } else {
                    // Add splits for the entire token range
                    addScanRangeSplitsForTokenRange(keyspace, cf, rack, tokenRange, desiredRecordsPerSplit,
                            allTokenRanges, builder);
                }
            }
        }

        return builder.build();
    }

    private void addScanRangeSplitsForTokenRange(CassandraKeyspace keyspace, ColumnFamily<ByteBuffer, DeltaKey> cf, String rack,
                                                 TokenRange tokenRange, int desiredRecordsPerSplit, Iterable<TokenRange> allTokenRanges,
                                                 ScanRangeSplits.Builder builder) {
        // Split the token range into sub-ranges with approximately the desired number of records per split
        String rangeStart = tokenRange.getStartToken();

        List<CfSplit> splits = getCfSplits(
                keyspace.getAstyanaxKeyspace(), cf, tokenRange.getStartToken(), tokenRange.getEndToken(),
                desiredRecordsPerSplit, allTokenRanges);

        for (CfSplit split : splits) {
            ByteBuffer begin = parseTokenString(split.getStartToken());
            ByteBuffer finish = parseTokenString(split.getEndToken());

            builder.addScanRange(rack, rangeStart, ScanRange.create(begin, finish));
        }
    }

    @Override
    public String getPlacementCluster(String placementName) {
        checkNotNull(placementName, "placement");

        DeltaPlacement placement = (DeltaPlacement) _placementCache.get(placementName);
        return placement.getKeyspace().getClusterName();
    }

    /**
     * Queries for records across multiple tables.  Data is returned in order by by shard then table.  If the caller
     * wants to get all rows for a table he will need to stitch the data together.
     */
    @Override
    public Iterator<MultiTableScanResult> multiTableScan(final MultiTableScanOptions query, final TableSet tables,
                                                         final LimitCounter limit, final ReadConsistency consistency, @Nullable DateTime cutoffTime) {
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
                                        query.isIncludeMirrorTables(), _maxColumnsRange.getLimit(), consistency, cutoffTime);
                            }
                        };
                    }
                })
                .iterator());

    }

    // DataCopyDAO
    @Override
    public void copy(AstyanaxStorage source, AstyanaxStorage dest, Runnable progress) {
        checkNotNull(source, "source");
        checkNotNull(dest, "dest");

        DeltaPlacement sourcePlacement = (DeltaPlacement) source.getPlacement();
        DeltaPlacement destPlacement = (DeltaPlacement) dest.getPlacement();

        // Loop over the source splits.
        Iterator<ByteBufferRange> scanIter = source.scanIterator(null);
        while (scanIter.hasNext()) {
            ByteBufferRange keyRange = scanIter.next();
            // Copy delta records in this split.
            copyRange(sourcePlacement, sourcePlacement.getBlockedDeltaColumnFamily(),
                    dest, destPlacement, destPlacement.getBlockedDeltaColumnFamily(),
                    _deltaKeyInc, keyRange, progress);
            // Copy audit records in this split.
            copyRange(sourcePlacement, sourcePlacement.getAuditColumnFamily(),
                    dest, destPlacement, destPlacement.getAuditColumnFamily(),
                    _uuidInc, keyRange, progress);
            // Copy delta history records in this split.
            copyRange(sourcePlacement, sourcePlacement.getDeltaHistoryColumnFamily(),
                    dest, destPlacement, destPlacement.getDeltaHistoryColumnFamily(),
                    _uuidInc, keyRange, progress);
        }
    }

    private <C> void copyRange(DeltaPlacement sourcePlacement, ColumnFamily<ByteBuffer, C> sourceCf,
                               AstyanaxStorage dest, DeltaPlacement destPlacement, ColumnFamily<ByteBuffer, C> destCf, ColumnInc<C> columnInc,
                               ByteBufferRange keyRange, Runnable progress) {
        ConsistencyLevel writeConsistency = SorConsistencies.toAstyanax(WriteConsistency.STRONG);

        Iterator<List<Row<ByteBuffer, C>>> rowsIter = Iterators.partition(
                rowScan(sourcePlacement, sourceCf, keyRange, _maxColumnsRange, LimitCounter.max(), ReadConsistency.STRONG),
                MAX_SCAN_ROWS_BATCH);
        int largeRowThreshold = _maxColumnsRange.getLimit();

        while (rowsIter.hasNext()) {
            List<Row<ByteBuffer, C>> rows = rowsIter.next();

            MutationBatch mutation = destPlacement.getKeyspace().prepareMutationBatch(writeConsistency);
            for (Row<ByteBuffer, C> row : rows) {
                ColumnList<C> columns = row.getColumns();

                // Map the source row key to the destination row key.  Its table uuid and shard key will be different.
                ByteBuffer newRowKey = dest.getRowKey(AstyanaxStorage.getContentKey(row.getRawKey()));

                // Copy the first N columns to the multi-row mutation.
                putAll(mutation.withRow(destCf, newRowKey), columns);

                // If this is a wide row, copy the remaining columns w/separate mutation objects.
                // This is especially common with the audit column family.
                if (columns.size() >= largeRowThreshold) {
                    C lastColumn = columns.getColumnByIndex(columns.size() - 1).getName();
                    Iterator<List<Column<C>>> columnsIter = Iterators.partition(
                            columnScan(row.getRawKey(), sourcePlacement, sourceCf, lastColumn, null,
                                    false, columnInc, Long.MAX_VALUE, 1, ReadConsistency.STRONG),
                            MAX_COLUMN_SCAN_BATCH);
                    while (columnsIter.hasNext()) {
                        List<Column<C>> moreColumns = columnsIter.next();

                        MutationBatch wideRowMutation = destPlacement.getKeyspace().prepareMutationBatch(writeConsistency);
                        putAll(wideRowMutation.withRow(destCf, newRowKey), moreColumns);
                        progress.run();
                        execute(wideRowMutation,
                                "copy key range %s to %s from placement %s, column family %s to placement %s, column family %s",
                                keyRange.getStart(), keyRange.getEnd(), sourcePlacement.getName(), sourceCf.getName(),
                                destPlacement.getName(), destCf.getName());
                    }
                }
            }
            progress.run();
            execute(mutation,
                    "copy key range %s to %s from placement %s, column family %s to placement %s, column family %s",
                    keyRange.getStart(), keyRange.getEnd(), sourcePlacement.getName(), sourceCf.getName(),
                    destPlacement.getName(), destCf.getName());

            _copyMeter.mark(rows.size());
        }
    }

    /**
     * Copies columns exactly, preserving Cassandra timestamp and ttl values.
     */
    private <C> void putAll(ColumnListMutation<C> mutation, Iterable<Column<C>> columns) {
        for (Column<C> column : columns) {
            mutation.setTimestamp(column.getTimestamp())
                    .putColumn(column.getName(), column.getByteBufferValue(), column.getTtl());
        }
    }

    /**
     * Queries for rows given an enumerated list of Cassandra row keys.
     */
    private Iterator<Record> rowQuery(DeltaPlacement placement,
                                      List<Map.Entry<ByteBuffer, Key>> keys,
                                      ReadConsistency consistency) {
        // Build the list of row IDs to query for.
        List<ByteBuffer> rowIds = Lists.transform(keys, entryKeyFunction());

        // Query for Delta & Compaction info, just the first 50 columns for now.
        final Rows<ByteBuffer, DeltaKey> rows = execute(placement.getKeyspace()
                        .prepareQuery(placement.getBlockedDeltaColumnFamily(), SorConsistencies.toAstyanax(consistency))
                        .getKeySlice(rowIds)
                        .withColumnRange(_maxColumnsRange),
                "query %d keys from placement %s", rowIds.size(), placement.getName());

        // Track metrics
        _randomReadMeter.mark(rowIds.size());

        // Return an iterator that decodes the row results, avoiding pinning multiple decoded rows into memory at once.
        return decodeRows(keys, rows, _maxColumnsRange.getLimit(), consistency);
    }

    /**
     * Scans for rows within the specified range, exclusive on start and inclusive on end.
     */
    private Iterator<Row<ByteBuffer, DeltaKey>> rowScan(final DeltaPlacement placement,
                                                        final ByteBufferRange rowRange,
                                                        final ByteBufferRange columnRange,
                                                        final LimitCounter limit,
                                                        final ReadConsistency consistency) {
        return rowScan(placement, placement.getBlockedDeltaColumnFamily(), rowRange, columnRange, limit, consistency);
    }

    /**
     * Scans for rows within the specified range, exclusive on start and inclusive on end.
     */
    private <P> Iterator<Row<ByteBuffer, P>> rowScan(final DeltaPlacement placement,
                                                     final ColumnFamily<ByteBuffer, P> columnFamily,
                                                     final ByteBufferRange rowRange,
                                                     final ByteBufferRange columnRange,
                                                     final LimitCounter limit,
                                                     final ReadConsistency consistency) {
        // In the first batch request no more than 50 rows.
        int initialBatchSize = (int) Math.min(limit.remaining(), 50);

        return new AbstractBatchReader<Row<ByteBuffer, P>>(1, initialBatchSize, MAX_SCAN_ROWS_BATCH, SCAN_ROW_BATCH_INCREMENT) {
            private ByteBuffer _rangeStart = rowRange.getStart();
            private final ByteBuffer _rangeEnd = rowRange.getEnd();
            private int _minimumLimit = 1;
            private boolean _done;

            @Override
            protected boolean hasNextBatch() {
                return !_done;
            }

            @Override
            protected Iterator<Row<ByteBuffer, P>> nextBatch(int batchSize)
                    throws Exception {
                // Note: if Cassandra is asked to perform a token range query where start >= end it will wrap
                // around which is absolutely *not* what we want since it could return data for another table.
                if (_done || BufferUtils.compareUnsigned(_rangeStart, _rangeEnd) >= 0) {
                    _done = true;
                    return Iterators.emptyIterator();
                }

                Timer.Context timer = _scanBatchTimer.time();
                try {
                    int adjustedBatchSize = (int) Math.min(Math.max(limit.remaining(), _minimumLimit), batchSize);
                    // Increase the minimum limit a bit each time around so if we start encountering lots of range
                    // ghosts we eventually scan through them at a reasonable rate.
                    _minimumLimit = Math.min(_minimumLimit + 3, MAX_SCAN_ROWS_BATCH);

                    // Pass token strings to get exclusive start behavior, to support 'fromBlobIdExclusive'.
                    String startToken = toTokenString(_rangeStart);
                    String endToken = toTokenString(_rangeEnd);
                    Rows<ByteBuffer, P> rows = execute(placement.getKeyspace()
                                    .prepareQuery(columnFamily, SorConsistencies.toAstyanax(consistency))
                                    .getKeyRange(null, null, startToken, endToken, adjustedBatchSize)
                                    .withColumnRange(columnRange),
                            "scan rows in placement %s, column family %s from %s to %s",
                            placement.getName(), columnFamily.getName(), startToken, endToken);

                    if (rows.size() >= adjustedBatchSize) {
                        // Save the last row key so we can use it as the start (exclusive) if we must query to get more data.
                        _rangeStart = rows.getRowByIndex(rows.size() - 1).getKey();
                        // If that row key was the end of our range then we're done.
                        _done = _rangeStart.equals(_rangeEnd);
                    } else {
                        // If we got fewer rows than we asked for, another query won't find more rows.
                        _done = true;
                    }

                    // Track metrics
                    _scanReadMeter.mark(rows.size());

                    // Return the rows.  Filter out range ghosts (deleted rows with no columns)
                    final Iterator<Row<ByteBuffer, P>> rowIter = rows.iterator();
                    return new AbstractIterator<Row<ByteBuffer, P>>() {
                        @Override
                        protected Row<ByteBuffer, P> computeNext() {
                            while (rowIter.hasNext()) {
                                Row<ByteBuffer, P> row = rowIter.next();
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

            @Override
            protected boolean isTimeoutException(Exception e) {
                return Iterables.tryFind(Throwables.getCausalChain(e), Predicates.instanceOf(IsTimeoutException.class)).isPresent();
            }

            @Override
            protected boolean isDataSizeException(Exception e) {
                for (Throwable t : Throwables.getCausalChain(e)) {
                    // If the root cause is a thrift frame size overflow then the current batch size returned too
                    // much data.  Unfortunately there is no specific exception thrown for this so we have to
                    // check for the generic exception type, TTransportException, and then narrow by the message.
                    //
                    // Sample message:
                    // Frame size (17339288) larger than max length (16384000)!
                    if (t instanceof TTransportException) {
                        String message = t.getMessage();
                        if (message != null &&
                                message.startsWith("Frame size") &&
                                message.contains("larger than max length")) {
                            return true;
                        }
                    }
                }
                return false;
            }
        };
    }


    /**
     * Scans a single row for columns within the specified range, inclusive or exclusive on start based on whether
     * <code>page</code> is non-zero, and inclusive on end.
     */
    private <C> Iterator<Column<C>> columnScan(final ByteBuffer rowKey,
                                               final DeltaPlacement placement,
                                               final ColumnFamily<ByteBuffer, C> columnFamily,
                                               final C start,
                                               final C end,
                                               final boolean reversed,
                                               final ColumnInc<C> columnInc,
                                               final long limit,
                                               final long page,
                                               final ReadConsistency consistency) {
        return Iterators.concat(new AbstractIterator<Iterator<Column<C>>>() {
            private C _from = start;
            private long _remaining = limit;
            private long _page = page;

            @Override
            protected Iterator<Column<C>> computeNext() {
                if (_remaining <= 0) {
                    return endOfData();
                }

                // For page N+1, treat "_from" as exclusive.  Since Cassandra doesn't support exclusive column ranges
                // bump the from value up to the next possible time UUID (assumes from != null when page != 0).
                if (_page > 0) {
                    if (_from.equals(end)) {
                        return endOfData();
                    }
                    _from = reversed ? columnInc.previous(_from) : columnInc.next(_from);
                    if (_from == null) {
                        return endOfData();
                    }
                }

                // Execute the query
                int batchSize = (int) Math.min(_remaining, MAX_COLUMN_SCAN_BATCH);
                ColumnList<C> columns = execute(placement.getKeyspace()
                                .prepareQuery(columnFamily, SorConsistencies.toAstyanax(consistency))
                                .getKey(rowKey)
                                .withColumnRange(_from, end, reversed, batchSize),
                        "scan columns in placement %s, column family %s, row %s, from %s to %s",
                        placement.getName(), columnFamily.getName(), rowKey, start, end);

                // Update state for the next iteration.
                if (columns.size() >= batchSize) {
                    // Save the last column key so we can use it as the start (exclusive) if we must query to get more data.
                    _from = columns.getColumnByIndex(columns.size() - 1).getName();
                    _remaining = _remaining - columns.size();
                    _page++;
                } else {
                    // If we got fewer columns than we asked for, another query won't find more columns.
                    _remaining = 0;
                }

                // Track metrics.  For rows w/more than 50 columns, count subsequent reads w/_largeRowReadMeter.
                (_page == 0 ? _randomReadMeter : _largeRowReadMeter).mark();

                return columns.iterator();
            }
        });
    }

    private interface ColumnInc<C> {
        C previous(C col);
        C next(C col);
    }

    private static final ColumnInc<UUID> _uuidInc = new ColumnInc<UUID>() {
        @Override
        public UUID previous(UUID col) {
            return TimeUUIDs.getPrevious(col);
        }

        @Override
        public UUID next(UUID col) {
            return TimeUUIDs.getNext(col);
        }
    };

    private static final ColumnInc<DeltaKey> _deltaKeyInc = new ColumnInc<DeltaKey>() {
        @Override
        public DeltaKey previous(DeltaKey col) {
            if (col.getBlock() == 0) {
                return new DeltaKey(_uuidInc.previous(col.getChangeId()), Integer.MAX_VALUE);
            }
            return new DeltaKey(col.getChangeId(), col.getBlock() - 1);
        }

        @Override
        public DeltaKey next(DeltaKey col) {
            if (col.getBlock() == Integer.MAX_VALUE) {
                return new DeltaKey(_uuidInc.next(col.getChangeId()), 0);
            }
            return new DeltaKey(col.getChangeId(), col.getBlock() + 1);
        }
    };

    /**
     * Decodes row keys returned by scanning a table.
     */
    private Iterator<String> decodeKeys(final Iterator<Row<ByteBuffer, DeltaKey>> iter) {
        return new AbstractIterator<String>() {
            @Override
            protected String computeNext() {
                while (iter.hasNext()) {
                    Row<ByteBuffer, DeltaKey> row = iter.next();
                    if (!row.getColumns().isEmpty()) { // Ignore range ghosts
                        return AstyanaxStorage.getContentKey(row.getRawKey());
                    }
                }
                return endOfData();
            }
        };
    }

    /**
     * Decodes rows returned by querying for a specific set of rows.
     */
    private Iterator<Record> decodeRows(List<Map.Entry<ByteBuffer, Key>> keys, final Rows<ByteBuffer, DeltaKey> rows,
                                        final int largeRowThreshold, final ReadConsistency consistency) {
        // Avoiding pinning multiple decoded rows into memory at once.
        return Iterators.transform(keys.iterator(), new Function<Map.Entry<ByteBuffer, Key>, Record>() {
            @Override
            public Record apply(Map.Entry<ByteBuffer, Key> entry) {
                Row<ByteBuffer, DeltaKey> row = rows.getRow(entry.getKey());
                if (row == null) {
                    return emptyRecord(entry.getValue());
                }
                // Convert the results into a Record object, lazily fetching the rest of the columns as necessary.
                return newRecord(entry.getValue(), row.getRawKey(), row.getColumns(), largeRowThreshold, consistency, null);
            }
        });
    }

    /**
     * Decodes rows returned by scanning a table.
     */
    private Iterator<Record> decodeRows(Iterator<Row<ByteBuffer, DeltaKey>> iter, final AstyanaxTable table,
                                        final int largeRowThreshold, final ReadConsistency consistency) {
        // Avoiding pinning multiple decoded rows into memory at once.
        return Iterators.transform(iter, new Function<Row<ByteBuffer, DeltaKey>, Record>() {
            @Override
            public Record apply(Row<ByteBuffer, DeltaKey> row) {
                // Convert the results into a Record object, lazily fetching the rest of the columns as necessary.
                String key = AstyanaxStorage.getContentKey(row.getRawKey());
                return newRecord(new Key(table, key), row.getRawKey(), row.getColumns(), largeRowThreshold, consistency, null);
            }
        });
    }

    /**
     * Decodes rows returned by scanning across tables.
     */
    private Iterator<MultiTableScanResult> scanMultiTableRows(
            final TableSet tables, final DeltaPlacement placement, final ByteBufferRange rowRange,
            final LimitCounter limit, final boolean includeDroppedTables, final boolean includeMirrorTables,
            final int largeRowThreshold, final ReadConsistency consistency, @Nullable final DateTime cutoffTime) {

        // Avoiding pinning multiple decoded rows into memory at once.
        return limit.limit(new AbstractIterator<MultiTableScanResult>() {
            private PeekingIterator<Row<ByteBuffer, DeltaKey>> _iter = Iterators.peekingIterator(
                    rowScan(placement, rowRange, _maxColumnsRange, LimitCounter.max(), consistency));

            private long _lastTableUuid = -1;
            private AstyanaxTable _table = null;
            private boolean _droppedTable;
            private boolean _primaryTable;

            @Override
            protected MultiTableScanResult computeNext() {
                while (_iter.hasNext()) {
                    Row<ByteBuffer, DeltaKey> row = _iter.next();
                    ColumnList<DeltaKey> rowColumns = row.getColumns();

                    // Convert the results into a Record object, lazily fetching the rest of the columns as necessary.
                    ByteBuffer rowKey = row.getRawKey();

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
                    Record record = newRecord(new Key(_table, key), rowKey, rowColumns, largeRowThreshold, consistency, cutoffTime);
                    return new MultiTableScanResult(rowKey, shardId, tableUuid, _droppedTable, record);
                }

                return endOfData();
            }

            private PeekingIterator<Row<ByteBuffer, DeltaKey>> skipToNextTable(long tableUuid) {
                // Iterate over the next 50 rows first to check for a table switch.  This avoids starting a new range
                // query if the number of rows in the undesired table is small.
                int skipLimit = 50;
                Row<ByteBuffer, DeltaKey> row = null;

                while (skipLimit != 0 && _iter.hasNext()) {
                    row = _iter.peek();
                    long nextTableUuid = AstyanaxStorage.getTableUuid(row.getRawKey());
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
                    assert row != null;
                    int shardId = AstyanaxStorage.getShardId(row.getRawKey());
                    ByteBuffer nextPossibleTableStart = AstyanaxStorage.getRowKeyRaw(shardId, tableUuid + 1, "");
                    ByteBuffer end = rowRange.getEnd();

                    if (AstyanaxStorage.compareKeys(nextPossibleTableStart, end) < 0) {
                        // We haven't reached the last end boundary of the original range scan
                        ByteBufferRange updatedRange = new ByteBufferRangeImpl(nextPossibleTableStart, end, -1, false);
                        return Iterators.peekingIterator(
                                rowScan(placement, updatedRange, _maxColumnsRange, LimitCounter.max(), consistency));
                    }
                }

                return Iterators.peekingIterator(Iterators.<Row<ByteBuffer, DeltaKey>>emptyIterator());
            }
        });
    }

    private Record newRecord(Key key, ByteBuffer rowKey, ColumnList<DeltaKey> columns, int largeRowThreshold, ReadConsistency consistency, @Nullable final DateTime cutoffTime) {

        Iterator<Column<DeltaKey>> changeIter = getFilteredColumnIter(columns.iterator(), cutoffTime);
        Iterator<Column<DeltaKey>> compactionIter = getFilteredColumnIter(columns.iterator(), cutoffTime);
        Iterator<Column<DeltaKey>> rawMetadataIter = getFilteredColumnIter(columns.iterator(), cutoffTime);

        if (columns.size() >= largeRowThreshold) {
            // A large row such that the first query likely returned only a subset of all the columns.  Lazily fetch
            // the rest while ensuring we never load all columns into memory at the same time.  The current
            // Compactor+Resolver implementation must scan the row twice: once to find compaction records and once to
            // find deltas.  So we must call columnScan() twice, once for each.
            DeltaKey lastColumn = columns.getColumnByIndex(columns.size() - 1).getName();

            AstyanaxTable table = (AstyanaxTable) key.getTable();
            AstyanaxStorage storage = table.getReadStorage();
            DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();
            ColumnFamily<ByteBuffer, DeltaKey> columnFamily = placement.getBlockedDeltaColumnFamily();

            // Execute the same scan 3 times, returning 3 iterators that process the results in different ways.  In
            // practice at most two of the iterators are actually consumed (one or more is ignored) so the columnScan
            // should avoid actually doing any work until the first item is fetched from the iterator.
            changeIter = Iterators.concat(changeIter,
                    getFilteredColumnIter(columnScan(rowKey, placement, columnFamily, lastColumn, null, false, _deltaKeyInc, Long.MAX_VALUE, 1, consistency), cutoffTime));
            compactionIter = Iterators.concat(compactionIter,
                    getFilteredColumnIter(columnScan(rowKey, placement, columnFamily, lastColumn, null, false, _deltaKeyInc, Long.MAX_VALUE, 1, consistency), cutoffTime));
            rawMetadataIter = Iterators.concat(rawMetadataIter,
                    getFilteredColumnIter(columnScan(rowKey, placement, columnFamily, lastColumn, null, false, _deltaKeyInc, Long.MAX_VALUE, 1, consistency), cutoffTime));
        }

        Iterator<Map.Entry<UUID, Change>> deltaChangeIter = decodeChanges(new AstyanaxDeltaIterator(changeIter, false, _deltaPrefixLength));
        Iterator<Map.Entry<UUID, Compaction>> deltaCompactionIter = decodeCompactions(new AstyanaxDeltaIterator(compactionIter, false, _deltaPrefixLength));
        Iterator<RecordEntryRawMetadata> deltaRawMetadataIter = rawMetadata(new AstyanaxDeltaIterator(rawMetadataIter, false, _deltaPrefixLength));

        return new RecordImpl(key, deltaCompactionIter, deltaChangeIter, deltaRawMetadataIter);
    }

    private Record emptyRecord(Key key) {
        return new RecordImpl(key,
                Iterators.<Map.Entry<UUID, Compaction>>emptyIterator(),
                Iterators.<Map.Entry<UUID, Change>>emptyIterator(),
                Iterators.<RecordEntryRawMetadata>emptyIterator());
    }

    private Iterator<Change> decodeColumns(Iterator<Column<UUID>> iter) {
        return Iterators.transform(iter, column -> _changeEncoder.decodeChange(column.getName(), column.getByteBufferValue()));
    }

    private Iterator<Change> decodeDeltaColumns(Iterator<Column<UUID>> iter) {
        return Iterators.transform(iter, column -> _changeEncoder.decodeChange(column.getName(), _daoUtils.skipPrefix(column.getByteBufferValue())));
    }

    private Iterator<Map.Entry<UUID, Change>> decodeChanges(final Iterator<Column<UUID>> iter) {
        return Iterators.transform(iter, new Function<Column<UUID>, Map.Entry<UUID, Change>>() {
            @Override
            public Map.Entry<UUID, Change> apply(Column<UUID> column) {
                Change change = _changeEncoder.decodeChange(column.getName(), _daoUtils.skipPrefix(column.getByteBufferValue()));
                return Maps.immutableEntry(column.getName(), change);
            }
        });
    }

    private Iterator<Map.Entry<UUID, Compaction>> decodeCompactions(final Iterator<Column<UUID>> iter) {
        return new AbstractIterator<Map.Entry<UUID, Compaction>>() {
            @Override
            protected Map.Entry<UUID, Compaction> computeNext() {
                while (iter.hasNext()) {
                    Column<UUID> column = iter.next();
                    Compaction compaction = _changeEncoder.decodeCompaction(_daoUtils.skipPrefix(column.getByteBufferValue()));
                    if (compaction != null) {
                        return Maps.immutableEntry(column.getName(), compaction);
                    }
                }
                return endOfData();
            }
        };
    }

    private Iterator<RecordEntryRawMetadata> rawMetadata(final Iterator<Column<UUID>> iter) {
        return Iterators.transform(iter, new Function<Column<UUID>, RecordEntryRawMetadata>() {
            @Override
            public RecordEntryRawMetadata apply(Column<UUID> column) {
                return new RecordEntryRawMetadata()
                        .withTimestamp(TimeUUIDs.getTimeMillis(column.getName()))
                        .withSize(_daoUtils.skipPrefix(column.getByteBufferValue()).remaining());
            }
        });
    }

    private <R> R execute(Execution<R> execution, String operation, Object... operationArguments) {
        OperationResult<R> operationResult;
        try {
            operationResult = execution.execute();
        } catch (ConnectionException e) {
            for (int i = 0; i < operationArguments.length; i++) {
                if (operationArguments[i] instanceof ByteBuffer) {
                    operationArguments[i] = ByteBufferUtil.bytesToHex((ByteBuffer) operationArguments[i]);
                }
            }
            String message = "Failed to " + String.format(operation, operationArguments);
            throw new RuntimeException(message, e);
        }
        return operationResult.getResult();
    }

    private String toTokenString(ByteBuffer bytes) {
        return _tokenFactory.toString(_tokenFactory.fromByteArray(bytes));
    }

    private ByteBuffer parseTokenString(String string) {
        return _tokenFactory.toByteArray(_tokenFactory.fromString(string));
    }

    /**
     * Force computation of the first item in an iterator so metrics calculations for a method reflect the cost of
     * the first batch of results.
     */
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

    @VisibleForTesting
    public static Iterator<Column<DeltaKey>> getFilteredColumnIter(Iterator<Column<DeltaKey>> columnIter, @Nullable DateTime cutoffTime) {
        if (cutoffTime == null) {
            return columnIter;
        }
        return Iterators.filter(columnIter, column -> (TimeUUIDs.getTimeMillis(column.getName().getChangeId()) < cutoffTime.getMillis()));
    }
}