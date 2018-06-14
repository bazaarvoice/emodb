package com.bazaarvoice.emodb.sor.db.astyanax;


import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.HistoryBatchPersister;
import com.bazaarvoice.emodb.sor.core.HistoryStore;
import com.bazaarvoice.emodb.sor.db.*;
import com.bazaarvoice.emodb.sor.db.cql.CqlWriterDAODelegate;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxTable;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public class CqlDataWriterDAO implements DataWriterDAO, MigratorWriterDAO {

    private final static int ROW_KEY_SIZE = 8;
    private final static int CHANGE_ID_SIZE = 8;
    private final static int MAX_STATEMENT_SIZE = 1 * 1024 * 1024; // 1Mb

    private final String _deltaPrefix;
    private final int _deltaPrefixLength;
    private final byte[] _deltaPrefixBytes;
    private final DAOUtils _daoUtils;
    private final boolean _writeToLegacyDeltaTable;
    private final boolean _writeToBlockedDeltaTable;

    private final DataWriterDAO _astyanaxWriterDAO;
    private final ChangeEncoder _changeEncoder;
    private final Meter _migratorMeter;
    private final Meter _blockedRowsMigratedMeter;
    private final PlacementCache _placementCache;

    private final HistoryStore _historyStore;

    @Inject
    public CqlDataWriterDAO(@CqlWriterDAODelegate DataWriterDAO delegate,
                            PlacementCache placementCache, HistoryStore historyStore,
                            ChangeEncoder changeEncoder, MetricRegistry metricRegistry,
                            DAOUtils daoUtils, @PrefixLength int deltaPrefixLength,
                            @WriteToLegacyDeltaTable boolean writeToLegacyDeltaTable, @WriteToBlockedDeltaTable boolean writeToBlockedDeltaTable) {

        checkArgument(writeToLegacyDeltaTable || writeToBlockedDeltaTable, "writeToLegacyDeltaTable and writeToBlockedDeltaTables cannot both be false");

        _astyanaxWriterDAO = checkNotNull(delegate, "delegate");
        _historyStore = checkNotNull(historyStore, "historyStore");
        _changeEncoder = checkNotNull(changeEncoder, "changeEncoder");
        _placementCache = placementCache;
        _migratorMeter = metricRegistry.meter(getMetricName("migratedRows"));
        _blockedRowsMigratedMeter = metricRegistry.meter(getMetricName("blockedMigratedRows"));
        _daoUtils = daoUtils;
        _deltaPrefix = StringUtils.repeat('0', deltaPrefixLength);
        _deltaPrefixBytes = _deltaPrefix.getBytes(Charsets.UTF_8);
        _deltaPrefixLength = deltaPrefixLength;
        _writeToLegacyDeltaTable = writeToLegacyDeltaTable;
        _writeToBlockedDeltaTable = writeToBlockedDeltaTable;
    }

    private String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.sor", "CqlDataWriterDAO", name);
    }

    @Override
    public long getFullConsistencyTimestamp(Table table) {
        return _astyanaxWriterDAO.getFullConsistencyTimestamp(table);
    }

    @Override
    public long getRawConsistencyTimestamp(Table table) {
        return _astyanaxWriterDAO.getRawConsistencyTimestamp(table);
    }

    @Override
    public void updateAll(Iterator<RecordUpdate> updates, UpdateListener listener) {
        _astyanaxWriterDAO.updateAll(updates, listener);
    }

    @Override
    public void compact(Table tbl, String key, UUID compactionKey, Compaction compaction, UUID changeId, Delta delta, Collection<UUID> changesToDelete, List<History> historyList, WriteConsistency consistency) {
        checkNotNull(tbl, "table");
        checkNotNull(key, "key");
        checkNotNull(compactionKey, "compactionKey");
        checkNotNull(compaction, "compaction");
        checkNotNull(changeId, "changeId");
        checkNotNull(delta, "delta");
        checkNotNull(changesToDelete, "changesToDelete");
        checkNotNull(consistency, "consistency");

        AstyanaxTable table = (AstyanaxTable) tbl;
        for (AstyanaxStorage storage : table.getWriteStorage()) {
            DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();
            CassandraKeyspace keyspace = placement.getKeyspace();

            ByteBuffer rowKey = storage.getRowKey(key);

            // Should synchronously write compaction and then delete deltas
            writeCompaction(rowKey, compactionKey, compaction, consistency, placement, keyspace);

            deleteCompactedDeltas(rowKey, consistency, placement, keyspace, changesToDelete, historyList);
        }
    }

    private void insertBlockedDeltas(BatchStatement batchStatement, BlockedDeltaTableDDL tableDDL, ConsistencyLevel consistencyLevel, ByteBuffer rowKey, UUID changeId, ByteBuffer encodedDelta) {

        List<ByteBuffer> blocks = _daoUtils.getDeltaBlocks(encodedDelta);

        if (blocks.size() > 1) {
            _blockedRowsMigratedMeter.mark();
        }

        for (int i = 0; i < blocks.size(); i++) {
            batchStatement.add(QueryBuilder.insertInto(tableDDL.getTableMetadata())
                    .value(tableDDL.getRowKeyColumnName(), rowKey)
                    .value(tableDDL.getChangeIdColumnName(), changeId)
                    .value(tableDDL.getBlockColumnName(), i)
                    .value(tableDDL.getValueColumnName(), blocks.get(i))
                    .setConsistencyLevel(consistencyLevel));
        }
    }

    private void writeCompaction(ByteBuffer rowKey, UUID compactionKey, Compaction compaction,
                                 WriteConsistency consistency, DeltaPlacement placement,
                                 CassandraKeyspace keyspace) {

        // Add the compaction record
        ByteBuffer encodedBlockedCompaction = ByteBuffer.wrap(_changeEncoder.encodeCompaction(compaction, new StringBuilder(_deltaPrefix)).toString().getBytes(Charsets.UTF_8));
        ByteBuffer encodedCompaction = encodedBlockedCompaction.duplicate();
        encodedCompaction.position(encodedCompaction.position() + _deltaPrefixLength);

        Session session = keyspace.getCqlSession();
        ConsistencyLevel consistencyLevel = SorConsistencies.toCql(consistency);

        ResultSetFuture oldTableFuture = null;

        // this write statement should be removed in the next version
        if (_writeToLegacyDeltaTable) {
            TableDDL deltaTableDDL = placement.getDeltaTableDDL();
            Statement oldTableStatement = QueryBuilder.insertInto(deltaTableDDL.getTableMetadata())
                    .value(deltaTableDDL.getRowKeyColumnName(), rowKey)
                    .value(deltaTableDDL.getChangeIdColumnName(), compactionKey)
                    .value(deltaTableDDL.getValueColumnName(), encodedCompaction)
                    .setConsistencyLevel(consistencyLevel);
            oldTableFuture = session.executeAsync(oldTableStatement);
        }


        if (_writeToBlockedDeltaTable) {
            // create new atomic batch
            BlockedDeltaTableDDL blockedDeltaTableDDL = placement.getBlockedDeltaTableDDL();
            BatchStatement newTableStatement = new BatchStatement(BatchStatement.Type.LOGGED);
            insertBlockedDeltas(newTableStatement, blockedDeltaTableDDL, consistencyLevel, rowKey, compactionKey, encodedBlockedCompaction);
            session.execute(newTableStatement);
        }

        // Wait for both statements to return
        if (oldTableFuture != null) {
            oldTableFuture.getUninterruptibly();
        }

    }

    private Statement deleteStatement(TableDDL tableDDL, ByteBuffer rowKey, UUID changeId, ConsistencyLevel consistencyLevel) {
        return QueryBuilder.delete()
                .from(tableDDL.getTableMetadata())
                .where(eq(tableDDL.getRowKeyColumnName(), rowKey))
                .and(eq(tableDDL.getChangeIdColumnName(), changeId))
                .setConsistencyLevel(consistencyLevel);
    }

    private void deleteCompactedDeltas(ByteBuffer rowKey, WriteConsistency consistency, DeltaPlacement placement,
                                       CassandraKeyspace keyspace, Collection<UUID> changesToDelete,
                                       List<History> historyList) {

        Session session = keyspace.getCqlSession();
        ConsistencyLevel consistencyLevel = SorConsistencies.toCql(consistency);

        // the old statement should be removed in the next version
        ResultSetFuture legacyTableFuture = null;
        ResultSetFuture historyBatchFuture = null;

        if (historyList != null && !historyList.isEmpty()) {
            BatchStatement historyBatchStatement = new BatchStatement();
            HistoryBatchPersister historyBatchPersister = CqlHistoryBatchPersister.build(historyBatchStatement,
                    placement.getDeltaHistoryTableDDL(), _changeEncoder, _historyStore, consistencyLevel);
            _historyStore.putDeltaHistory(rowKey, historyList, historyBatchPersister);
            historyBatchFuture = session.executeAsync(historyBatchStatement);
        }

        // delete the old deltas & compaction records
        if (_writeToLegacyDeltaTable) {
            BatchStatement legacyTableStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for (UUID change : changesToDelete) {
                legacyTableStatement.add(deleteStatement(placement.getDeltaTableDDL(), rowKey, change, consistencyLevel));
            }
            legacyTableFuture = session.executeAsync(legacyTableStatement);
        }

        if (_writeToBlockedDeltaTable) {
            BatchStatement newTableStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for (UUID change : changesToDelete) {
                newTableStatement.add(deleteStatement(placement.getBlockedDeltaTableDDL(), rowKey, change, consistencyLevel));
            }
            session.execute(newTableStatement);
        }

        if (legacyTableFuture != null) {
            legacyTableFuture.getUninterruptibly();
        }

        if (historyBatchFuture != null) {
            historyBatchFuture.getUninterruptibly();
        }
    }

    @Override
    public void storeCompactedDeltas(Table tbl, String key, List<History> histories, WriteConsistency consistency) {
        _astyanaxWriterDAO.storeCompactedDeltas(tbl, key, histories, consistency);
    }

    @Override
    public void purgeUnsafe(Table table) {
        _astyanaxWriterDAO.purgeUnsafe(table);
    }

    private void checkError(AtomicReference<Throwable> t) {
        if (t.get() != null) {
            throw Throwables.propagate(t.get());
        }
    }

    @Override
    public void writeRows(String placementName, Iterator<MigrationScanResult> iterator, RateLimiter rateLimiter) {

        if (!iterator.hasNext()) {
            return;
        }

        DeltaPlacement placement = (DeltaPlacement) _placementCache.get(placementName);
        Session session = placement.getKeyspace().getCqlSession();
        BatchStatement statement = new BatchStatement(BatchStatement.Type.LOGGED);
        AtomicReference<Throwable> error = new AtomicReference<>();
        Phaser phaser = new Phaser();
        ByteBuffer lastRowKey = null;
        int currentStatementSize = 0;
        FutureCallback<ResultSet> callback = new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(@Nullable ResultSet result) {
                phaser.arriveAndDeregister();
            }

            @Override
            public void onFailure(Throwable t) {
                phaser.arriveAndDeregister();
                error.compareAndSet(null, t);
            }
        };

        while (iterator.hasNext()) {
            MigrationScanResult result = iterator.next();

            ByteBuffer rowKey = result.getRowKey();

            // build blocked delta value
            ByteBuffer delta = result.getValue();
            int deltaSize = delta.remaining();
            ByteBuffer encodedDelta = ByteBuffer.allocate(deltaSize + _deltaPrefixLength);
            encodedDelta.put(_deltaPrefixBytes);
            encodedDelta.put(delta);
            encodedDelta.position(0);

            _migratorMeter.mark();

            // execute statement if we have encountered a new C* wide row OR if statement has become too large
            if ((lastRowKey != null && !rowKey.equals(lastRowKey)) || currentStatementSize > MAX_STATEMENT_SIZE) {

                rateLimiter.acquire();
                phaser.register();

                Futures.addCallback(session.executeAsync(statement), callback);
                statement = new BatchStatement(BatchStatement.Type.LOGGED);
                currentStatementSize = 0;
            }

            lastRowKey = rowKey;
            insertBlockedDeltas(statement, placement.getBlockedDeltaTableDDL(), ConsistencyLevel.LOCAL_QUORUM, rowKey, result.getChangeId(), encodedDelta);
            currentStatementSize += encodedDelta.limit() + ROW_KEY_SIZE + CHANGE_ID_SIZE;

            // fail fast if an error has occurred
            checkError(error);

        }

        rateLimiter.acquire();
        phaser.register();
        Futures.addCallback(session.executeAsync(statement), callback);
        phaser.arriveAndAwaitAdvance();
        checkError(error);

    }
}