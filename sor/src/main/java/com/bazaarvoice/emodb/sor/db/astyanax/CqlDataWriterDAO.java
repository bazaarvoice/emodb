package com.bazaarvoice.emodb.sor.db.astyanax;


import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.HistoryBatchPersister;
import com.bazaarvoice.emodb.sor.core.HistoryStore;
import com.bazaarvoice.emodb.sor.db.DAOUtils;
import com.bazaarvoice.emodb.sor.db.DataWriterDAO;
import com.bazaarvoice.emodb.sor.db.HistoryMigrationScanResult;
import com.bazaarvoice.emodb.sor.db.MigrationScanResult;
import com.bazaarvoice.emodb.sor.db.RecordUpdate;
import com.bazaarvoice.emodb.sor.db.cql.CqlWriterDAODelegate;
import com.bazaarvoice.emodb.sor.db.test.DeltaClusteringKey;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxTable;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static java.util.Objects.requireNonNull;


public class CqlDataWriterDAO implements DataWriterDAO, DataCopyWriterDAO {

    private final static int ROW_KEY_SIZE = 8;
    private final static int CHANGE_ID_SIZE = 8;
    private final static int MAX_STATEMENT_SIZE = 1 * 1024 * 1024; // 1Mb

    private final String _deltaPrefix;
    private final int _deltaPrefixLength;
    private final byte[] _deltaPrefixBytes;
    private final DAOUtils _daoUtils;
    private final boolean _cellTombstoneCompactionEnabled;
    private final int _cellTombstoneBlockLimit;

    private final DataWriterDAO _astyanaxWriterDAO;
    private final ChangeEncoder _changeEncoder;
    private final Meter _blockedRowsMigratedMeter;

    private final HistoryStore _historyStore;

    @Inject
    public CqlDataWriterDAO(@CqlWriterDAODelegate DataWriterDAO delegate,
                            PlacementCache placementCache, HistoryStore historyStore,
                            ChangeEncoder changeEncoder, MetricRegistry metricRegistry,
                            DAOUtils daoUtils, @PrefixLength int deltaPrefixLength,
                            @CellTombstoneCompactionEnabled boolean cellTombstoneCompactionEnabled, @CellTombstoneBlockLimit int cellTombstoneBlockLimit) {


        _astyanaxWriterDAO = requireNonNull(delegate, "delegate");
        _historyStore = requireNonNull(historyStore, "historyStore");
        _changeEncoder = requireNonNull(changeEncoder, "changeEncoder");
        _blockedRowsMigratedMeter = metricRegistry.meter(getMetricName("blockedMigratedRows"));
        _daoUtils = daoUtils;
        _deltaPrefix = StringUtils.repeat('0', deltaPrefixLength);
        _deltaPrefixBytes = _deltaPrefix.getBytes(Charsets.UTF_8);
        _deltaPrefixLength = deltaPrefixLength;
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
        requireNonNull(tbl, "table");
        requireNonNull(key, "key");
        requireNonNull(compactionKey, "compactionKey");
        requireNonNull(compaction, "compaction");
        requireNonNull(changeId, "changeId");
        requireNonNull(delta, "delta");
        requireNonNull(changesToDelete, "changesToDelete");
        requireNonNull(consistency, "consistency");

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
        Session session = keyspace.getCqlSession();
        ConsistencyLevel consistencyLevel = SorConsistencies.toCql(consistency);

        // create new atomic batch
        BlockedDeltaTableDDL blockedDeltaTableDDL = placement.getBlockedDeltaTableDDL();
        BatchStatement newTableStatement = new BatchStatement(BatchStatement.Type.LOGGED);
        insertBlockedDeltas(newTableStatement, blockedDeltaTableDDL, consistencyLevel, rowKey, compactionKey, encodedBlockedCompaction);
        session.execute(newTableStatement);

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
        ResultSetFuture historyBatchFuture = null;

        if (historyList != null && !historyList.isEmpty()) {
            BatchStatement historyBatchStatement = new BatchStatement();
            HistoryBatchPersister historyBatchPersister = CqlHistoryBatchPersister.build(historyBatchStatement,
                    placement.getDeltaHistoryTableDDL(), _changeEncoder, _historyStore, consistencyLevel);
            _historyStore.putDeltaHistory(rowKey, historyList, historyBatchPersister);
            historyBatchFuture = session.executeAsync(historyBatchStatement);
        }

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

            ByteBuffer blockedDelta = ByteBuffer.allocate(deltaSize + _deltaPrefixLength);
            blockedDelta.put(_deltaPrefixBytes);
            blockedDelta.put(delta.duplicate());
            blockedDelta.position(0);
            insertBlockedDeltas(newTableBatchStatment, placement.getBlockedDeltaTableDDL(),
                    SorConsistencies.toCql(WriteConsistency.STRONG), rowKey, result.getChangeId(), blockedDelta);

            currentStatementSize += deltaSize + ROW_KEY_SIZE + CHANGE_ID_SIZE;
        }

        executeAndReplaceIfNotEmpty(oldTableBatchStatement, session, progress);
        executeAndReplaceIfNotEmpty(newTableBatchStatment, session, progress);
    }
}