package com.bazaarvoice.emodb.sor.db.astyanax;


import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.AuditBatchPersister;
import com.bazaarvoice.emodb.sor.core.AuditStore;
import com.bazaarvoice.emodb.sor.db.*;
import com.bazaarvoice.emodb.sor.db.cql.CqlWriterDAODelegate;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxTable;
import com.bazaarvoice.emodb.table.db.astyanax.FullConsistencyTimeProvider;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementCache;
import com.bazaarvoice.emodb.table.db.consistency.HintsConsistencyTimeProvider;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;



public class CqlDataWriterDAO implements DataWriterDAO, MigratorWriterDAO {

    private final static int ROW_KEY_SIZE = 8;
    private final static int CHANGE_ID_SIZE = 8;
    private final static int MAX_STATEMENT_SIZE = 1 * 1024 * 1024; // 1Mb

    private final int _deltaBlockSize;
    private final String _deltaPrefix;
    private final int _deltaPrefixLength;
    private final byte[] _deltaPrefixBytes;
    private final DAOUtils _daoUtils;

    private final AstyanaxDataReaderDAO _readerDao;
    private final DataWriterDAO _astyanaxWriterDAO;
    private final ChangeEncoder _changeEncoder;
    private final Meter _migratorMeter;
    private final Meter _blockedRowsMigratedMeter;
    private final FullConsistencyTimeProvider _fullConsistencyTimeProvider;
    private final PlacementCache _placementCache;

    private final HintsConsistencyTimeProvider _rawConsistencyTimeProvider;
    private final AuditStore _auditStore;

    @Inject
    public CqlDataWriterDAO(@CqlWriterDAODelegate DataWriterDAO delegate, AstyanaxDataReaderDAO readerDao,
                            PlacementCache placementCache, FullConsistencyTimeProvider fullConsistencyTimeProvider,
                            AuditStore auditStore, HintsConsistencyTimeProvider rawConsistencyTimeProvider,
                            ChangeEncoder changeEncoder, MetricRegistry metricRegistry, DAOUtils daoUtils,
                            @BlockSize int deltaBlockSize, @PrefixLength int deltaPrefixLength) {
        _readerDao = checkNotNull(readerDao, "readerDao");
        _astyanaxWriterDAO = checkNotNull(delegate, "delegate");
        _fullConsistencyTimeProvider = checkNotNull(fullConsistencyTimeProvider, "fullConsistencyTimeProvider");
        _rawConsistencyTimeProvider = checkNotNull(rawConsistencyTimeProvider, "rawConsistencyTimeProvider");
        _auditStore = checkNotNull(auditStore, "auditStore");
        _changeEncoder = checkNotNull(changeEncoder, "changeEncoder");
        _placementCache = placementCache;
        _migratorMeter = metricRegistry.meter(getMetricName("migratedRows"));
        _blockedRowsMigratedMeter = metricRegistry.meter(getMetricName("blockedMigratedRows"));
        _daoUtils = daoUtils;
        _deltaBlockSize = deltaBlockSize;
        _deltaPrefix = StringUtils.repeat('0', deltaPrefixLength);
        _deltaPrefixBytes = _deltaPrefix.getBytes(Charsets.UTF_8);
        _deltaPrefixLength = deltaPrefixLength;
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
        _astyanaxWriterDAO.updateAll(updates,listener);
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

        // TODO: implement checks to ensure we are under the transport size OR drastically increase batch_size_warn_in_kb

        // Add the compaction record
        ByteBuffer encodedBlockedCompaction = ByteBuffer.wrap(_changeEncoder.encodeCompaction(compaction, new StringBuilder(_deltaPrefix)).toString().getBytes(Charsets.UTF_8));
        ByteBuffer encodedCompaction = encodedBlockedCompaction.duplicate();
        encodedCompaction.position(encodedCompaction.position() + _deltaPrefixLength);

        Session session = keyspace.getCqlSession();
        ConsistencyLevel consistencyLevel = SorConsistencies.toCql(consistency);

        TableDDL deltaTableDDL = placement.getDeltaTableDDL();
        BlockedDeltaTableDDL blockedDeltaTableDDL = placement.getBlockedDeltaTableDDL();

        // create new atomic batch
        BatchStatement newTableStatement = new BatchStatement(BatchStatement.Type.LOGGED);

        insertBlockedDeltas(newTableStatement, blockedDeltaTableDDL, consistencyLevel, rowKey, compactionKey, encodedBlockedCompaction);

        ResultSetFuture newTableFuture = session.executeAsync(newTableStatement);

        // this write statement should be removed in the next version
        Statement oldTableStatement = QueryBuilder.insertInto(deltaTableDDL.getTableMetadata())
                .value(deltaTableDDL.getRowKeyColumnName(), rowKey)
                .value(deltaTableDDL.getChangeIdColumnName(), compactionKey)
                .value(deltaTableDDL.getValueColumnName(), encodedCompaction)
                .setConsistencyLevel(consistencyLevel);

        ResultSetFuture oldTableFuture =  session.executeAsync(oldTableStatement);

        // Wait for both statements to return
        newTableFuture.getUninterruptibly();
        oldTableFuture.getUninterruptibly();
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

        // delete the old deltas & compaction records

        TableDDL blockedDeltaTableDDL = placement.getBlockedDeltaTableDDL();
        TableDDL deltaTableDDL = placement.getDeltaTableDDL();

        // the old statement should be removed in the next version
        BatchStatement oldTableStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        BatchStatement newTableStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);

        for (UUID change : changesToDelete) {
            oldTableStatement.add(deleteStatement(deltaTableDDL, rowKey, change, consistencyLevel));
            newTableStatement.add(deleteStatement(blockedDeltaTableDDL, rowKey, change, consistencyLevel));
        }


        if (historyList != null && !historyList.isEmpty()) {
            AuditBatchPersister auditBatchPersister = CqlAuditBatchPersister.build(oldTableStatement, placement.getDeltaHistoryTableDDL(),
                    _changeEncoder, _auditStore);
            _auditStore.putDeltaAudits(rowKey, historyList, auditBatchPersister);
        }

        ResultSetFuture oldTableFuture =  session.executeAsync(oldTableStatement);
        ResultSetFuture newTableFuture = session.executeAsync(newTableStatement);

        // Wait for both statements to return
        newTableFuture.getUninterruptibly();
        oldTableFuture.getUninterruptibly();
    }

    @Override
    public void storeCompactedDeltas(Table tbl, String key, List<History> audits, WriteConsistency consistency) {
        _astyanaxWriterDAO.storeCompactedDeltas(tbl, key, audits, consistency);
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
    public void writeRows(String placementName, Iterator<MigrationScanResult> iterator, int maxConcurrentWrites) {

        if (!iterator.hasNext()) {
            return;
        }

        DeltaPlacement placement = (DeltaPlacement) _placementCache.get(placementName);
        Session session = placement.getKeyspace().getCqlSession();
        BatchStatement statement = new BatchStatement(BatchStatement.Type.LOGGED);
        final Semaphore semaphore = new Semaphore(maxConcurrentWrites);
        AtomicReference<Throwable> error = new AtomicReference<>();
        ByteBuffer lastRowKey = null;
        int currentStatementSize = 0;
        FutureCallback<ResultSet> callback = new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(@Nullable ResultSet result) {
                semaphore.release();
            }

            @Override
            public void onFailure(Throwable t) {
                semaphore.release();
                error.compareAndSet(null, t);
            }
        };

        while(iterator.hasNext()) {
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

                semaphore.acquireUninterruptibly();
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

        semaphore.acquireUninterruptibly();
        Futures.addCallback(session.executeAsync(statement), callback);
        semaphore.acquireUninterruptibly(maxConcurrentWrites);
        checkError(error);

    }
}