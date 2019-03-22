package com.bazaarvoice.emodb.sor.db.astyanax;


import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.HistoryBatchPersister;
import com.bazaarvoice.emodb.sor.core.HistoryStore;
import com.bazaarvoice.emodb.sor.db.*;
import com.bazaarvoice.emodb.sor.db.cql.CqlWriterDAODelegate;
import com.bazaarvoice.emodb.sor.db.test.DeltaClusteringKey;
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
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;


public class CqlDataWriterDAO implements DataWriterDAO, MigratorWriterDAO, DataCopyWriterDAO {

    private final static int ROW_KEY_SIZE = 8;
    private final static int CHANGE_ID_SIZE = 8;
    private final static int MAX_STATEMENT_SIZE = 1 * 1024 * 1024; // 1Mb

    private final String _deltaPrefix;
    private final int _deltaPrefixLength;
    private final byte[] _deltaPrefixBytes;
    private final DAOUtils _daoUtils;
    private final boolean _writeToLegacyDeltaTable;
    private final boolean _writeToBlockedDeltaTable;
    private final boolean _cellTombstoneCompactionEnabled;
    private final int _cellTombstoneBlockLimit;

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
                            @WriteToLegacyDeltaTable boolean writeToLegacyDeltaTable, @WriteToBlockedDeltaTable boolean writeToBlockedDeltaTable,
                            @CellTombstoneCompactionEnabled boolean cellTombstoneCompactionEnabled, @CellTombstoneBlockLimit int cellTombstoneBlockLimit) {

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
        _cellTombstoneCompactionEnabled = cellTombstoneCompactionEnabled;
        _cellTombstoneBlockLimit = cellTombstoneBlockLimit;
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
    public void compact(Table tbl, String key, UUID compactionKey, Compaction compaction, UUID changeId, Delta delta, Collection<DeltaClusteringKey> changesToDelete, List<History> historyList, WriteConsistency consistency) {
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

    /**
     * This method returns a delete statement for a single block, rather than the delta as whole. In order to delete an
     * entire delta using this method, it should be called once for each block. In cases in which there are relatively
     * few blocks in a delta, this method can be more efficient than {@link #deleteStatement(TableDDL, ByteBuffer, UUID, ConsistencyLevel)},
     * as the cell tombstones created by this method are more efficient than range tombstones
     */
    private Statement blockDeleteStatement(BlockedDeltaTableDDL tableDDL, ByteBuffer rowKey, UUID changeId, int block, ConsistencyLevel consistencyLevel) {
        return QueryBuilder.delete()
                .from(tableDDL.getTableMetadata())
                .where(eq(tableDDL.getRowKeyColumnName(), rowKey))
                .and(eq(tableDDL.getChangeIdColumnName(), changeId))
                .and(eq(tableDDL.getBlockColumnName(), block))
                .setConsistencyLevel(consistencyLevel);
    }

    private void deleteCompactedDeltas(ByteBuffer rowKey, WriteConsistency consistency, DeltaPlacement placement,
                                       CassandraKeyspace keyspace, Collection<DeltaClusteringKey> changesToDelete,
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
            for (DeltaClusteringKey change : changesToDelete) {
                legacyTableStatement.add(deleteStatement(placement.getDeltaTableDDL(), rowKey, change.getChangeId(), consistencyLevel));
            }
            legacyTableFuture = session.executeAsync(legacyTableStatement);
        }

        if (_writeToBlockedDeltaTable) {
            BatchStatement newTableStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for (DeltaClusteringKey change : changesToDelete) {
                if (_cellTombstoneCompactionEnabled && change.getNumBlocks() <= _cellTombstoneBlockLimit) {
                    for (int i = 0; i < change.getNumBlocks(); i++) {
                        newTableStatement.add(blockDeleteStatement(placement.getBlockedDeltaTableDDL(), rowKey, change.getChangeId(), i, consistencyLevel));
                    }
                }
                else {
                    newTableStatement.add(deleteStatement(placement.getBlockedDeltaTableDDL(), rowKey, change.getChangeId(), consistencyLevel));
                }
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

    private BatchStatement executeAndReplaceIfNotEmpty(BatchStatement batchStatement, Session session,
                                                       Runnable progress) {
        if (batchStatement.size() > 0) {
            progress.run();
            session.execute(batchStatement);
            return new BatchStatement(BatchStatement.Type.LOGGED);
        }
        return batchStatement;
    }

    @Override
    public void copyHistoriesToDestination(Iterator<? extends HistoryMigrationScanResult> rows, AstyanaxStorage dest, Runnable progress) {
        if (!rows.hasNext()) {
            return;
        }

        DeltaPlacement placement = (DeltaPlacement) dest.getPlacement();
        Session session = placement.getKeyspace().getCqlSession();

        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.LOGGED);

        ByteBuffer lastRowKey = null;
        int currentStatementSize = 0;

        while (rows.hasNext()) {
            HistoryMigrationScanResult result = rows.next();

            ByteBuffer rowKey = dest.getRowKey(AstyanaxStorage.getContentKey(result.getRowKey()));

            ByteBuffer history = result.getValue();
            int historySize = history.remaining();

            if ((lastRowKey != null && !rowKey.equals(lastRowKey)) || currentStatementSize > MAX_STATEMENT_SIZE) {
                batchStatement = executeAndReplaceIfNotEmpty(batchStatement, session, progress);
            }

            lastRowKey = rowKey;

            TableDDL historyTableDDL = placement.getDeltaHistoryTableDDL();
            Statement oldTableStatement = QueryBuilder.insertInto(historyTableDDL.getTableMetadata())
                    .value(historyTableDDL.getRowKeyColumnName(), rowKey)
                    .value(historyTableDDL.getChangeIdColumnName(), result.getChangeId())
                    .value(historyTableDDL.getValueColumnName(), history)
                    .using(ttl(result.getTtl()))
                    .setConsistencyLevel(SorConsistencies.toCql(WriteConsistency.STRONG));
            batchStatement.add(oldTableStatement);

            currentStatementSize += historySize + ROW_KEY_SIZE + CHANGE_ID_SIZE;
        }

        executeAndReplaceIfNotEmpty(batchStatement, session, progress);
    }

    @Override
    public void copyDeltasToDestination(Iterator<? extends MigrationScanResult> rows, AstyanaxStorage dest, Runnable progress) {
        if (!rows.hasNext()) {
            return;
        }

        DeltaPlacement placement = (DeltaPlacement) dest.getPlacement();
        Session session = placement.getKeyspace().getCqlSession();

        BatchStatement oldTableBatchStatement = new BatchStatement(BatchStatement.Type.LOGGED);
        BatchStatement newTableBatchStatment = new BatchStatement(BatchStatement.Type.LOGGED);

        ByteBuffer lastRowKey = null;
        int currentStatementSize = 0;

        while (rows.hasNext()) {
            MigrationScanResult result = rows.next();

            ByteBuffer rowKey = dest.getRowKey(AstyanaxStorage.getContentKey(result.getRowKey()));

            ByteBuffer delta = result.getValue();
            int deltaSize = delta.remaining();

            if ((lastRowKey != null && !rowKey.equals(lastRowKey)) || currentStatementSize > MAX_STATEMENT_SIZE) {
                oldTableBatchStatement = executeAndReplaceIfNotEmpty(oldTableBatchStatement, session, progress);
                newTableBatchStatment = executeAndReplaceIfNotEmpty(newTableBatchStatment, session, progress);
            }

            lastRowKey = rowKey;

            if (_writeToLegacyDeltaTable) {
                TableDDL deltaTableDDL = placement.getDeltaTableDDL();
                Statement oldTableStatement = QueryBuilder.insertInto(deltaTableDDL.getTableMetadata())
                        .value(deltaTableDDL.getRowKeyColumnName(), rowKey)
                        .value(deltaTableDDL.getChangeIdColumnName(), result.getChangeId())
                        .value(deltaTableDDL.getValueColumnName(), delta)
                        .setConsistencyLevel(SorConsistencies.toCql(WriteConsistency.STRONG));
                oldTableBatchStatement.add(oldTableStatement);
            }

            if (_writeToBlockedDeltaTable) {
                ByteBuffer blockedDelta = ByteBuffer.allocate(deltaSize + _deltaPrefixLength);
                blockedDelta.put(_deltaPrefixBytes);
                blockedDelta.put(delta.duplicate());
                blockedDelta.position(0);
                insertBlockedDeltas(newTableBatchStatment, placement.getBlockedDeltaTableDDL(),
                        SorConsistencies.toCql(WriteConsistency.STRONG), rowKey, result.getChangeId(), blockedDelta);
            }

            currentStatementSize += deltaSize + ROW_KEY_SIZE + CHANGE_ID_SIZE;
        }

        executeAndReplaceIfNotEmpty(oldTableBatchStatement, session, progress);
        executeAndReplaceIfNotEmpty(newTableBatchStatment, session, progress);
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
        Phaser phaser = new Phaser(1);
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