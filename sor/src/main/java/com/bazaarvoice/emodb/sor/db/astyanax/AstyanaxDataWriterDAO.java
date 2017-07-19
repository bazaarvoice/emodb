package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.api.Ttls;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuditBuilder;
import com.bazaarvoice.emodb.sor.api.AuditSizeLimitException;
import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.DeltaSizeLimitException;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.core.AuditBatchPersister;
import com.bazaarvoice.emodb.sor.core.AuditStore;
import com.bazaarvoice.emodb.sor.db.DAOUtils;
import com.bazaarvoice.emodb.sor.db.DataWriterDAO;
import com.bazaarvoice.emodb.sor.db.RecordUpdate;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.sor.delta.Literal;
import com.bazaarvoice.emodb.sor.delta.MapDelta;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxStorage;
import com.bazaarvoice.emodb.table.db.astyanax.AstyanaxTable;
import com.bazaarvoice.emodb.table.db.astyanax.DataPurgeDAO;
import com.bazaarvoice.emodb.table.db.astyanax.FullConsistencyTimeProvider;
import com.bazaarvoice.emodb.table.db.consistency.HintsConsistencyTimeProvider;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.AbstractThriftMutationBatchImpl;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cassandra implementation of {@link DataWriterDAO} that uses the Netflix Astyanax client library.
 */
public class AstyanaxDataWriterDAO implements DataWriterDAO, DataPurgeDAO {
    private static final int MAX_BATCH_SIZE = 100;
    private static final int MAX_PENDING_SIZE = 200;
    // Must match thrift_framed_transport_size_in_mb value from cassandra.yaml
    private static final int MAX_THRIFT_FRAMED_TRANSPORT_SIZE = 15 * 1024 * 1024;
    // Because of the thrift framed transport size conservatively limit the size of deltas and audits
    // to allow ample room for additional metadata and protocol overhead.
    private static final int MAX_DELTA_SIZE = 10 * 1024 * 1024;   // 10 MB delta limit, measured in UTF-8 bytes
    private static final int MAX_AUDIT_SIZE = 1 * 1024 * 1024;    // 1 MB audit limit, measured in UTF-8 bytes

    private final AstyanaxDataReaderDAO _readerDao;
    private final DataWriterDAO _cqlWriterDAO;
    private final ChangeEncoder _changeEncoder;
    private final Meter _updateMeter;
    private final Meter _oversizeUpdateMeter;
    private final FullConsistencyTimeProvider _fullConsistencyTimeProvider;
    private final int _deltaBlockSize;
    private final String _deltaPrefix;
    private final int _deltaPrefixLength;

    // The difference between full consistency and "raw" consistency provider is that full consistency also includes
    //  a minimum lag of 5 minutes, whereas "raw" consistency timestamp just gives us the last known good FCT which could be less than 5 minutes.
    // We use this for efficiency reasons, the only use case right now is to delete "compaction-owned" deltas, once we
    //  know that compaction is within FCT.
    private final HintsConsistencyTimeProvider _rawConsistencyTimeProvider;
    private final AuditStore _auditStore;

    @Inject
    public AstyanaxDataWriterDAO(@AstyanaxWriterDAODelegate DataWriterDAO delegate, AstyanaxDataReaderDAO readerDao,
                                 FullConsistencyTimeProvider fullConsistencyTimeProvider, AuditStore auditStore,
                                 HintsConsistencyTimeProvider rawConsistencyTimeProvider,
                                 ChangeEncoder changeEncoder,
                                 MetricRegistry metricRegistry,
                                 @BlockSize int deltaBlockSize,
                                 @PrefixLength int deltaPrefixLength) {
        _cqlWriterDAO = checkNotNull(delegate, "delegate");
        _readerDao = checkNotNull(readerDao, "readerDao");
        _fullConsistencyTimeProvider = checkNotNull(fullConsistencyTimeProvider, "fullConsistencyTimeProvider");
        _rawConsistencyTimeProvider = checkNotNull(rawConsistencyTimeProvider, "rawConsistencyTimeProvider");
        _auditStore = checkNotNull(auditStore, "auditStore");
        _changeEncoder = checkNotNull(changeEncoder, "changeEncoder");
        _updateMeter = metricRegistry.meter(getMetricName("updates"));
        _oversizeUpdateMeter = metricRegistry.meter(getMetricName("oversizeUpdates"));
        _deltaBlockSize = deltaBlockSize;
        _deltaPrefix = StringUtils.repeat('0', deltaPrefixLength);
        _deltaPrefixLength = deltaPrefixLength;
    }

    private String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.sor", "AstyanaxDataWriterDAO", name);
    }

    @Override
    public long getFullConsistencyTimestamp(Table tbl) {
        return getFullConsistencyTimestamp((AstyanaxTable)tbl, _fullConsistencyTimeProvider);
    }

    @Override
    public long getRawConsistencyTimestamp(Table tbl) {
        return getFullConsistencyTimestamp((AstyanaxTable)tbl, _rawConsistencyTimeProvider);
    }

    private long getFullConsistencyTimestamp(AstyanaxTable tbl, FullConsistencyTimeProvider fullConsistencyTimeProvider) {
        // Compaction runs off the "read" storage.  If there are multiple back-end write storage configurations,
        // we don't care whether the secondary is falling behind, only the primary that we read from matters.
        DeltaPlacement placement = (DeltaPlacement) tbl.getReadStorage().getPlacement();
        String clusterName = placement.getKeyspace().getClusterName();
        return fullConsistencyTimeProvider.getMaxTimeStamp(clusterName);
    }

    @Timed(name = "bv.emodb.sor.AstyanaxDataWriterDAO.updateAll", absolute = true)
    @Override
    public void updateAll(Iterator<RecordUpdate> updates, UpdateListener listener) {
        Map<BatchKey, List<BatchUpdate>> batchMap = Maps.newLinkedHashMap();
        int numPending = 0;

        // Group the updates by distinct placement and consistency since a Cassandra mutation only works
        // with a single keyspace and consistency at a time.
        while (updates.hasNext()) {
            RecordUpdate update = updates.next();

            AstyanaxTable table = (AstyanaxTable) update.getTable();
            for (AstyanaxStorage storage : table.getWriteStorage()) {
                DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();

                BatchKey batchKey = new BatchKey(placement, update.getConsistency());
                List<BatchUpdate> batch = batchMap.get(batchKey);
                if (batch == null) {
                    batchMap.put(batchKey, batch = Lists.newArrayList());
                }
                batch.add(new BatchUpdate(storage, update));
                numPending++;

                // Flush this batch if it's bigger than the maximum mutation we want to send to Cassandra.  Alternatively,
                // don't queue more than MAX_PENDING_SIZE updates in memory at a time, to keep max mem usage down.  Go
                // ahead and flush all the batches at once, even if some are still small, in order to avoid potentially
                // extreme re-ordering of writes (say a batch contains 1 record in placement A followed by 100k records in
                // placement B, we shouldn't delay writing A until after all B records).
                if (batch.size() >= MAX_BATCH_SIZE || numPending >= MAX_PENDING_SIZE) {
                    writeAll(batchMap, listener);
                    batchMap.clear();
                    numPending = 0;
                }
            }
        }

        // Flush final batches.
        writeAll(batchMap, listener);
    }

    private void writeAll(Map<BatchKey, List<BatchUpdate>> batchMap, UpdateListener listener) {
        for (Map.Entry<BatchKey, List<BatchUpdate>> entry : batchMap.entrySet()) {
            write(entry.getKey(), entry.getValue(), listener);
        }
    }

    private void putBlockedDeltaColumn(ColumnListMutation mutation, UUID changeId, ByteBuffer encodedDelta) {
        List<ByteBuffer> blocks = DAOUtils.getBlockedDeltas(encodedDelta, _deltaPrefixLength, _deltaBlockSize);
        for (int i = 0; i < blocks.size(); i++) {
            mutation.putColumn(new DeltaKey(changeId, i), blocks.get(i));
        }
    }

    private void write(BatchKey batchKey, List<BatchUpdate> updates, UpdateListener listener) {
        // Invoke the configured listener.  This is used to write events to the databus.
        listener.beforeWrite(Collections2.transform(updates, new Function<BatchUpdate, RecordUpdate>() {
            @Override
            public RecordUpdate apply(BatchUpdate update) {
                return update.getUpdate();
            }
        }));

        DeltaPlacement placement = batchKey.getPlacement();
        MutationBatch mutation = placement.getKeyspace().prepareMutationBatch(batchKey.getConsistency());
        int approxMutationSize = 0;
        int updateCount = 0;

        for (BatchUpdate batchUpdate : updates) {
            AstyanaxStorage storage = batchUpdate.getStorage();
            RecordUpdate update = batchUpdate.getUpdate();
            ByteBuffer rowKey = storage.getRowKey(update.getKey());

            Delta delta = update.getDelta();
            String deltaString = delta.toString();
            Set<String> tags = update.getTags();

            // Set any change flags which may make reading this delta back more efficient.  Currently the only case
            // for this is for a literal map delta.
            EnumSet<ChangeFlag> changeFlags = EnumSet.noneOf(ChangeFlag.class);
            if (delta.isConstant()) {
                changeFlags.add(ChangeFlag.CONSTANT_DELTA);
            }
            if (delta instanceof MapDelta || (delta instanceof Literal && ((Literal) delta).getValue() instanceof Map)) {
                changeFlags.add(ChangeFlag.MAP_DELTA);
            }

            // Add the hash of the delta to the audit log to make it easy to tell when the same delta is written multiple times
            // Update the audit to include the tags associated with the update
            Audit augmentedAudit = AuditBuilder.from(update.getAudit())
                    .set(Audit.SHA1, Hashing.sha1().hashUnencodedChars(deltaString).toString())
                    .set(Audit.TAGS, tags)
                    .build();

            // The values are encoded in a flexible format that allows versioning of the strings
            ByteBuffer encodedBlockDelta = stringToByteBuffer(_changeEncoder.encodeDelta(deltaString, changeFlags, tags, new StringBuilder(_deltaPrefix)));
            ByteBuffer encodedDelta = encodedBlockDelta.duplicate();
            encodedDelta.position(encodedDelta.position() + _deltaPrefixLength);
            ByteBuffer encodedAudit = stringToByteBuffer(_changeEncoder.encodeAudit(augmentedAudit));

            int deltaSize = encodedDelta.remaining();
            int blockDeltaSize = encodedBlockDelta.remaining();
            int auditSize = encodedAudit.remaining();

            UUID changeId = update.getChangeId();

            // Validate sizes of individual deltas and audits
            if (deltaSize > MAX_DELTA_SIZE) {
                _oversizeUpdateMeter.mark();
                throw new DeltaSizeLimitException("Delta exceeds size limit of " + MAX_DELTA_SIZE + ": " + deltaSize, deltaSize);
            }
            if (auditSize > MAX_AUDIT_SIZE) {
                _oversizeUpdateMeter.mark();
                throw new AuditSizeLimitException("Audit exceeds size limit of " + MAX_AUDIT_SIZE + ": " + auditSize, auditSize);
            }

            // Perform a quick validation that the size of the mutation batch as a whole won't exceed the thrift threshold.
            // This validation is inexact and overly-conservative but it is cheap and fast.
            if (!mutation.isEmpty() && approxMutationSize + deltaSize + blockDeltaSize + auditSize > MAX_DELTA_SIZE + MAX_AUDIT_SIZE) {
                // Adding the next row may exceed the Thrift threshold.  Check definitively now.  This is fairly expensive
                // which is why we don't do it unless the cheap check above passes.
                MutationBatch potentiallyOversizeMutation = placement.getKeyspace().prepareMutationBatch(batchKey.getConsistency());
                potentiallyOversizeMutation.mergeShallow(mutation);

                potentiallyOversizeMutation.withRow(placement.getDeltaColumnFamily(), rowKey).putColumn(changeId, encodedDelta, null);
                potentiallyOversizeMutation.withRow(placement.getAuditColumnFamily(), rowKey).putColumn(changeId, encodedAudit, null);
                putBlockedDeltaColumn(potentiallyOversizeMutation.withRow(placement.getBlockedDeltaColumnFamily(), rowKey), changeId, encodedBlockDelta);

                if (getMutationBatchSize(potentiallyOversizeMutation) >= MAX_THRIFT_FRAMED_TRANSPORT_SIZE) {
                    // Execute the mutation batch now.  As a side-effect this empties the mutation batch
                    // so we can continue using the same instance.
                    execute(mutation, "batch update %d records in placement %s", updateCount, placement.getName());
                    approxMutationSize = 0;
                    updateCount = 0;
                }
            }

            mutation.withRow(placement.getDeltaColumnFamily(), rowKey).putColumn(changeId, encodedDelta, null);
            mutation.withRow(placement.getAuditColumnFamily(), rowKey).putColumn(changeId, encodedAudit, null);
            putBlockedDeltaColumn(mutation.withRow(placement.getBlockedDeltaColumnFamily(), rowKey), changeId, encodedBlockDelta);
            approxMutationSize += deltaSize + auditSize;
            updateCount += 1;
        }
        execute(mutation, "batch update %d records in placement %s", updateCount, placement.getName());

        _updateMeter.mark(updates.size());
    }

    private ByteBuffer stringToByteBuffer(String str) {
        return StringSerializer.get().toByteBuffer(str);
    }

    /**
     * We need to make sure that compaction is written *before* the compacted deltas are deleted.
     * This should be a synchronous operation.
     */
    @Timed(name = "bv.emodb.sor.AstyanaxDataWriterDAO.compact", absolute = true)
    @Override
    public void compact(Table tbl, String key, UUID compactionKey, Compaction compaction, UUID changeId,
                        Delta delta, Collection<UUID> changesToDelete, List<History> historyList, WriteConsistency consistency) {
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
            writeCompaction(rowKey, compactionKey, compaction, consistency, placement, keyspace, tbl, key);

            deleteCompactedDeltas(rowKey, consistency, placement, keyspace, changesToDelete, historyList, tbl, key);
        }
    }

    private void writeCompaction(ByteBuffer rowKey, UUID compactionKey, Compaction compaction,
                                 WriteConsistency consistency, DeltaPlacement placement,
                                 CassandraKeyspace keyspace, Table table, String key) {
            MutationBatch mutation = keyspace.prepareMutationBatch(SorConsistencies.toAstyanax(consistency));
            ColumnListMutation<UUID> rowMutation = mutation.withRow(placement.getDeltaColumnFamily(), rowKey);


        // TODO: ensure that thrift transport size is not breached here
        ByteBuffer encodedBlockCompaction = stringToByteBuffer(_changeEncoder.encodeCompaction(compaction, new StringBuilder(_deltaPrefix)));
        ByteBuffer encodedCompaction = encodedBlockCompaction.duplicate();
        encodedCompaction.position(encodedCompaction.position() + _deltaPrefixLength);

        // Add the compaction record
        rowMutation.putColumn(compactionKey, encodedCompaction, null);
        putBlockedDeltaColumn(mutation.withRow((placement.getBlockedDeltaColumnFamily()), rowKey), compactionKey, encodedBlockCompaction);
        // Write the new compaction
        execute(mutation, "compact placement %s, table %s, key %s", placement.getName(), table.getName(), key);
    }

    private void deleteCompactedDeltas(ByteBuffer rowKey, WriteConsistency consistency, DeltaPlacement placement,
                                       CassandraKeyspace keyspace, Collection<UUID> changesToDelete,
                                       List<History> historyList, Table table, String key) {
        MutationBatch mutation = keyspace.prepareMutationBatch(SorConsistencies.toAstyanax(consistency));
        ColumnListMutation<UUID> rowMutation = mutation.withRow(placement.getDeltaColumnFamily(), rowKey);

        // delete the old deltas & compaction records
        for (UUID change : changesToDelete) {
            rowMutation.deleteColumn(change);
        }

        // Archive compacted deltas
        if (historyList != null && !historyList.isEmpty()) {
            AuditBatchPersister auditBatchPersister =
                    AstyanaxAuditBatchPersister.build(mutation, placement.getDeltaHistoryColumnFamily(),
                            _changeEncoder, _auditStore);
            _auditStore.putDeltaAudits(rowKey, historyList, auditBatchPersister);
        }

        execute(mutation, "compact placement %s, table %s, key %s", placement.getName(), table.getName(), key);

    }

    @Timed (name = "bv.emodb.sorAstyanaxDataWriterDAO.storeCompactedDeltas", absolute = true)
    @Override
    public void storeCompactedDeltas(Table tbl, String key, List<History> audits, WriteConsistency consistency) {
        checkNotNull(tbl, "table");
        checkNotNull(key, "key");
        checkNotNull(audits, "audits");
        checkNotNull(consistency, "consistency");

        AstyanaxTable table = (AstyanaxTable) tbl;
        for (AstyanaxStorage storage : table.getWriteStorage()) {
            DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();
            CassandraKeyspace keyspace = placement.getKeyspace();

            ByteBuffer rowKey = storage.getRowKey(key);

            MutationBatch mutation = keyspace.prepareMutationBatch(SorConsistencies.toAstyanax(consistency));
            ColumnListMutation<UUID> rowMutation = mutation.withRow(placement.getDeltaHistoryColumnFamily(), rowKey);

            for (History history : audits) {
                rowMutation.putColumn(history.getChangeId(),
                        _changeEncoder.encodeHistory(history),
                        Ttls.toSeconds(_auditStore.getHistoryTtl(), 1, null));
            }
            execute(mutation, "store %d compacted deltas for placement %s, table %s, key %s",
                    audits.size(), placement.getName(), table.getName(), key);
        }
    }

    @Timed(name = "bv.emodb.sor.AstyanaxDataWriterDAO.purgeUnsafe", absolute = true)
    @Override
    public void purgeUnsafe(Table tbl) {
        checkNotNull(tbl, "table");

        AstyanaxTable table = (AstyanaxTable) tbl;
        for (AstyanaxStorage storage : table.getWriteStorage()) {
            purge(storage, noop());
        }
    }

    // DataPurgeDAO
    @Override
    public void purge(AstyanaxStorage storage, Runnable progress) {
        DeltaPlacement placement = (DeltaPlacement) storage.getPlacement();
        CassandraKeyspace keyspace = placement.getKeyspace();

        // Scan all the shards and delete all the rows we find.
        MutationBatch mutation = keyspace.prepareMutationBatch(SorConsistencies.toAstyanax(WriteConsistency.STRONG));
        Iterator<String> keyIter = _readerDao.scanKeys(storage, ReadConsistency.STRONG);
        while (keyIter.hasNext()) {
            ByteBuffer rowKey = storage.getRowKey(keyIter.next());
            mutation.withRow(placement.getDeltaColumnFamily(), rowKey).delete();
            mutation.withRow(placement.getBlockedDeltaColumnFamily(), rowKey).delete();
            mutation.withRow(placement.getAuditColumnFamily(), rowKey).delete();
            if (mutation.getRowCount() >= 100) {
                progress.run();
                execute(mutation, "purge %d records from placement %s", mutation.getRowCount(), placement.getName());
                mutation.discardMutations();
            }
        }
        if (!mutation.isEmpty()) {
            progress.run();
            execute(mutation, "purge %d records from placement %s", mutation.getRowCount(), placement.getName());
        }
    }

    private <R> R execute(Execution<R> execution, String operation, Object... operationArguments) {
        OperationResult<R> operationResult;
        try {
            operationResult = execution.execute();
        } catch (ConnectionException e) {
            String message = String.format(operation, operationArguments);
            if (isThriftFramedTransportSizeOverrun(execution, e)) {
                throw new ThriftFramedTransportSizeException("Thrift request to large to " + message, e);
            }
            throw new RuntimeException("Failed to " + message, e);
        }
        return operationResult.getResult();
    }

    private boolean isThriftFramedTransportSizeOverrun(Execution<?> execution, ConnectionException exception) {
        // Thrift framed transport size overruns don't have an explicit exception, but they fall under the general
        // umbrella of "unknown" thrift transport exceptions.
        Optional<Throwable> thriftException =
                Iterables.tryFind(Throwables.getCausalChain(exception), Predicates.instanceOf(TTransportException.class));
        //noinspection ThrowableResultOfMethodCallIgnored
        if (!thriftException.isPresent() || ((TTransportException) thriftException.get()).getType() != TTransportException.UNKNOWN) {
            return false;
        }

        return execution instanceof MutationBatch &&
                getMutationBatchSize((MutationBatch) execution) >= MAX_THRIFT_FRAMED_TRANSPORT_SIZE;
    }

    private int getMutationBatchSize(MutationBatch mutation) {
        assert mutation instanceof AbstractThriftMutationBatchImpl : "MutationBatch is not an instance of AbstractThriftMutationBatchImpl";
        try (CountingOutputStream countingOut = new CountingOutputStream(ByteStreams.nullOutputStream())) {
            TIOStreamTransport transport = new TIOStreamTransport(countingOut);
            Cassandra.batch_mutate_args args = new Cassandra.batch_mutate_args();
            args.setMutation_map(((AbstractThriftMutationBatchImpl) mutation).getMutationMap());

            args.write(new TBinaryProtocol(transport));
            return (int) countingOut.getCount();
        } catch (TException | IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private Runnable noop() {
        return new Runnable() {
            @Override
            public void run() {
                // Do nothing
            }
        };
    }

    /** Key used for grouping batches of update operations for execution. */
    private static class BatchKey {
        private final DeltaPlacement _placement;
        private final ConsistencyLevel _consistency;

        BatchKey(DeltaPlacement placement, WriteConsistency consistency) {
            _placement = placement;
            _consistency = SorConsistencies.toAstyanax(consistency);
        }

        DeltaPlacement getPlacement() {
            return _placement;
        }

        ConsistencyLevel getConsistency() {
            return _consistency;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BatchKey)) {
                return false;
            }
            BatchKey batchKey = (BatchKey) o;
            return _consistency == batchKey.getConsistency() &&
                    _placement.equals(batchKey.getPlacement());
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(_placement, _consistency);
        }
    }

    /** Value used for grouping batches of update operations for execution. */
    private static class BatchUpdate {
        private final AstyanaxStorage _storage;
        private final RecordUpdate _update;

        BatchUpdate(AstyanaxStorage storage, RecordUpdate record) {
            _storage = storage;
            _update = record;
        }

        AstyanaxStorage getStorage() {
            return _storage;
        }

        RecordUpdate getUpdate() {
            return _update;
        }
    }
}
