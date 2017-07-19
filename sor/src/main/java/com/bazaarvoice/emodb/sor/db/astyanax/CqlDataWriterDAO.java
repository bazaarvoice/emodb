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
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Named;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;



public class CqlDataWriterDAO implements DataWriterDAO, MigratorWriterDAO {

    private final int _deltaBlockSize;
    private final String _deltaPrefix;
    private final int _deltaPrefixLength;
    private final byte[] _deltaPrefixBytes;

    private final AstyanaxDataReaderDAO _readerDao;
    private final DataWriterDAO _astyanaxWriterDAO;
    private final ChangeEncoder _changeEncoder;
    private final Meter _updateMeter;
    private final Meter _oversizeUpdateMeter;
    private final FullConsistencyTimeProvider _fullConsistencyTimeProvider;
    private final PlacementCache _placementCache;

    private final HintsConsistencyTimeProvider _rawConsistencyTimeProvider;
    private final AuditStore _auditStore;

    @Inject
    public CqlDataWriterDAO(@CqlWriterDAODelegate DataWriterDAO delegate, AstyanaxDataReaderDAO readerDao,
                            PlacementCache placementCache, FullConsistencyTimeProvider fullConsistencyTimeProvider,
                            AuditStore auditStore, HintsConsistencyTimeProvider rawConsistencyTimeProvider,
                            ChangeEncoder changeEncoder, MetricRegistry metricRegistry, @BlockSize int deltaBlockSize,
                            @PrefixLength int deltaPrefixLength) {
        _readerDao = checkNotNull(readerDao, "readerDao");
        _astyanaxWriterDAO = checkNotNull(delegate, "delegate");
        _fullConsistencyTimeProvider = checkNotNull(fullConsistencyTimeProvider, "fullConsistencyTimeProvider");
        _rawConsistencyTimeProvider = checkNotNull(rawConsistencyTimeProvider, "rawConsistencyTimeProvider");
        _auditStore = checkNotNull(auditStore, "auditStore");
        _changeEncoder = checkNotNull(changeEncoder, "changeEncoder");
        _placementCache = placementCache;
        _updateMeter = metricRegistry.meter(getMetricName("updates"));
        _oversizeUpdateMeter = metricRegistry.meter(getMetricName("oversizeUpdates"));
        _deltaBlockSize = deltaBlockSize;
        _deltaPrefix = StringUtils.repeat('0', deltaPrefixLength);
        _deltaPrefixBytes = _deltaPrefix.getBytes();
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

        List<ByteBuffer> blocks = DAOUtils.getBlockedDeltas(encodedDelta, _deltaPrefixLength, _deltaBlockSize);

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

        // TODO: implement checks to ensure we are under the transport size

        // Add the compaction record
        ByteBuffer encodedBlockedCompaction = ByteBuffer.wrap(_changeEncoder.encodeCompaction(compaction, new StringBuilder(_deltaPrefix)).getBytes());
        ByteBuffer encodedCompaction = encodedBlockedCompaction.duplicate();
        encodedCompaction.position(encodedCompaction.position() + _deltaPrefixLength);

        int blockedDeltaSize = encodedCompaction.remaining();

        Session session = keyspace.getCqlSession();
        ConsistencyLevel consistencyLevel = SorConsistencies.toCql(consistency);

        TableDDL deltaTableDDL = placement.getDeltaTableDDL();
        BlockedDeltaTableDDL blockedDeltaTableDDL = placement.getBlockedDeltaTableDDL();

        BatchStatement batchStatement = new BatchStatement(); // may need a type in the constructor!

        batchStatement.add(QueryBuilder.insertInto(deltaTableDDL.getTableMetadata())
                .value(deltaTableDDL.getRowKeyColumnName(), rowKey)
                .value(deltaTableDDL.getChangeIdColumnName(), compactionKey)
                .value(deltaTableDDL.getValueColumnName(), encodedCompaction)
                .setConsistencyLevel(consistencyLevel));

        insertBlockedDeltas(batchStatement, blockedDeltaTableDDL, consistencyLevel, rowKey, compactionKey, encodedBlockedCompaction);

        session.execute(batchStatement);
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

        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);

        for (UUID change : changesToDelete) {
            batchStatement.add(deleteStatement(blockedDeltaTableDDL, rowKey, change, consistencyLevel));
            batchStatement.add(deleteStatement(deltaTableDDL, rowKey, change, consistencyLevel));
        }


        if (historyList != null && !historyList.isEmpty()) {
            AuditBatchPersister auditBatchPersister = CqlAuditBatchPersister.build(batchStatement, placement.getDeltaHistoryTableDDL(),
                    _changeEncoder, _auditStore);
            _auditStore.putDeltaAudits(rowKey, historyList, auditBatchPersister);
        }

        session.execute(batchStatement);

    }

    @Override
    public void storeCompactedDeltas(Table tbl, String key, List<History> audits, WriteConsistency consistency) {
        _astyanaxWriterDAO.storeCompactedDeltas(tbl, key, audits, consistency);
    }

    @Override
    public void purgeUnsafe(Table table) {
        _astyanaxWriterDAO.purgeUnsafe(table);
    }

    @Override
    public void writeRows(String placementName, Iterator<MigrationScanResult> results) {
        DeltaPlacement placement = (DeltaPlacement) _placementCache.get(placementName);
        Session session = placement.getKeyspace().getCqlSession();
        PeekingIterator<MigrationScanResult> iterator = Iterators.peekingIterator(results);

        int batchSize = 0;

        // nothing to write so return immediately
        if (!iterator.hasNext()) {
            return;
        }

        BatchStatement batchStatement = new BatchStatement();

        while (true) {
            MigrationScanResult result = iterator.next();
            ByteBuffer delta = result.getValue();
            int deltaSize = delta.remaining();
            ByteBuffer encodedDelta = ByteBuffer.allocate(deltaSize + _deltaPrefixLength);
            encodedDelta.put(_deltaPrefixBytes);
            encodedDelta.position(_deltaPrefixLength);
            encodedDelta.put(delta);
            encodedDelta.position(0);
            batchSize += encodedDelta.remaining() + 16;
            ByteBuffer rowKey = result.getRowKey();
            insertBlockedDeltas(batchStatement, placement.getBlockedDeltaTableDDL(), ConsistencyLevel.LOCAL_QUORUM, rowKey, result.getChangeId(), encodedDelta);

            if (!iterator.hasNext()) {
                break;
            }

            if (batchSize > 5 * 1024 || !iterator.peek().getRowKey().equals(rowKey)) {
                session.execute(batchStatement);
                batchStatement = new BatchStatement();
                batchSize = 0;
            }
        }

        session.execute(batchStatement);
    }
}