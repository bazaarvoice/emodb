package com.bazaarvoice.emodb.sor.test;

import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.db.DataWriterDAO;
import com.bazaarvoice.emodb.sor.db.RecordUpdate;
import com.bazaarvoice.emodb.sor.db.test.DeltaClusteringKey;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.table.db.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/**
 * Reads and writes to a "local" DataWriterDAO and forwards write operations to
 * a set of "remote" DataDAOs.
 * <p>
 * Replication can be stopped and started to simulate replication lag in
 * unit tests.
 * </p>
 * <p>
 *     Replication can be replayed out of order too ...
 * </p>
 */
public class ReplicatingDataWriterDAO implements DataWriterDAO {

    private final DataWriterDAO _local;
    private final List<PausableDataWriterDAO> _remotes = Lists.newArrayList();

    public ReplicatingDataWriterDAO(DataWriterDAO local) {
        _local = local;
    }

    public void replicateTo(DataWriterDAO... remotes) {
        for (DataWriterDAO remote : remotes) {
            if (remote instanceof ReplicatingDataWriterDAO) {
                remote = ((ReplicatingDataWriterDAO) remote).getLocal();
            }
            _remotes.add(new PausableDataWriterDAO(remote));
        }
    }

    public DataWriterDAO getLocal() {
        return _local;
    }

    @Override
    public long getFullConsistencyTimestamp(Table table) {
        return _local.getFullConsistencyTimestamp(table);
    }

    @Override
    public long getRawConsistencyTimestamp(Table table) {
        return _local.getRawConsistencyTimestamp(table);
    }

    @Override
    public void updateAll(Iterator<RecordUpdate> updates, UpdateListener listener) {
        ImmutableList<RecordUpdate> list = ImmutableList.copyOf(updates);
        _local.updateAll(list.iterator(), listener);
        for (DataWriterDAO remote : _remotes) {
            remote.updateAll(list.iterator(), noop());
        }
    }

    @Override
    public void compact(Table table, String key, UUID compactionKey, Compaction compaction, UUID changeId, Delta delta,
                        Collection<DeltaClusteringKey> changesToDelete, List<History> historyList, WriteConsistency consistency) {
        _local.compact(table, key, compactionKey, compaction, changeId, delta, changesToDelete, historyList, consistency);
        for (DataWriterDAO remote : _remotes) {
            remote.compact(table, key, compactionKey, compaction, changeId, delta, changesToDelete, historyList, consistency);
        }
    }

    @Override
    public void storeCompactedDeltas(Table tbl, String key, List<History> histories, WriteConsistency consistency) {
        _local.storeCompactedDeltas(tbl, key, histories, consistency);
        for (DataWriterDAO remote : _remotes) {
            remote.storeCompactedDeltas(tbl, key, histories, consistency);
        }
    }

    @Override
    public void purgeUnsafe(Table table) {
        _local.purgeUnsafe(table);
        for (DataWriterDAO remote : _remotes) {
            remote.purgeUnsafe(table);
        }
    }

    public void stopReplication() {
        for (PausableDataWriterDAO remote : _remotes) {
            remote.pause();
        }
    }

    public void startReplication() {
        for (PausableDataWriterDAO remote : _remotes) {
            remote.unpause();
        }
    }

    public void onlyReplicateDeletesUponCompaction() {
        for (PausableDataWriterDAO remote : _remotes) {
            remote.onlyReplicateDeletesUponCompaction();
        }
    }

    public void replicateCompactionDeltas() {
        for (PausableDataWriterDAO remote : _remotes) {
            remote.replicateCompactionDeltas();
        }
    }

    private UpdateListener noop() {
        return new UpdateListener() {
            @Override
            public void beforeWrite(Collection<RecordUpdate> updates) { }

            @Override
            public void afterWrite(Collection<RecordUpdate> updates) { }
        };
    }
}
