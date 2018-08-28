package com.bazaarvoice.emodb.sor.test;

import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.db.DataWriterDAO;
import com.bazaarvoice.emodb.sor.db.DeltaUpdate;
import com.bazaarvoice.emodb.sor.db.test.InMemoryDataReaderDAO;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.table.db.Table;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.UUID;

/**
 * Wraps an instance of {@link DataWriterDAO} and adds the ability to delay write operations.
 * This is useful for simulating replication delays.
 */
public class PausableDataWriterDAO implements DataWriterDAO {

    private final DataWriterDAO _delegate;
    private final Queue<Runnable> _writeQueue = Lists.newLinkedList();
    private final Queue<Runnable> _addCompactQueue = Lists.newLinkedList();
    private boolean _paused;
    private boolean _onlyReplicateDeletesUponCompaction;

    public PausableDataWriterDAO(DataWriterDAO delegate) {
        _delegate = delegate;
    }

    @Override
    public long getFullConsistencyTimestamp(Table table) {
        return _delegate.getFullConsistencyTimestamp(table);
    }

    @Override
    public long getRawConsistencyTimestamp(Table table) {
        return _delegate.getRawConsistencyTimestamp(table);
    }

    @Override
    public void updateAll(final Iterator<DeltaUpdate> updates, final UpdateListener listener) {
        write(() -> _delegate.updateAll(updates, listener));
    }

    @Override
    public void compact(final Table table, final String key, @Nullable final UUID compactionKey, @Nullable final Compaction compaction, @Nullable final UUID changeId, @Nullable final Delta delta,
                        final Collection<UUID> changesToDelete, final List<History> historyList, final WriteConsistency consistency) {
        if (_onlyReplicateDeletesUponCompaction) {
            ((InMemoryDataReaderDAO)_delegate).deleteDeltasOnly(table, key, compactionKey, compaction, changeId, delta,
                    changesToDelete, historyList, consistency);
            _addCompactQueue.add(() -> ((InMemoryDataReaderDAO)_delegate).addCompactionOnly(table, key, compactionKey,
                    compaction, changeId, delta, changesToDelete, historyList, consistency));
            return;
        }
        write(() -> _delegate.compact(table, key, compactionKey, compaction, changeId, delta, changesToDelete, historyList, consistency));
    }

    @Override
    public void storeCompactedDeltas(final Table tbl, final String key, final List<History> histories, final WriteConsistency consistency) {
        write(new Runnable() {
            @Override
            public void run() {
                _delegate.storeCompactedDeltas(tbl, key, histories, consistency);
            }
        });
    }

    @Override
    public void purgeUnsafe(final Table table) {
        write(new Runnable() {
            @Override
            public void run() {
                _delegate.purgeUnsafe(table);
            }
        });
    }

    /**
     * Pauses writes.  Write operations will queue until {@link #unpause()} is called, at which point
     * they will be executed in order.
     */
    public synchronized void pause() {
        _paused = true;
    }

    /**
     * Only replicate deletes due to compactions. Replicating compactions will be queued for later
     */
    public synchronized void onlyReplicateDeletesUponCompaction() {
        _onlyReplicateDeletesUponCompaction = true;
    }

    /**
     * Executes queued compaction deltas and unpauses compaction delta replication.
     */
    public synchronized void replicateCompactionDeltas() {
        _onlyReplicateDeletesUponCompaction = false;
        while (!_addCompactQueue.isEmpty()) {
            _addCompactQueue.remove().run();
        }
    }

    /**
     * Executes queued writes and unpauses future writes such that they execute without delay.
     */
    public synchronized void unpause() {
        _paused = false;
        while (!_writeQueue.isEmpty()) {
            _writeQueue.remove().run();
        }
    }

    private synchronized void write(Runnable runnable) {
        if (_paused) {
            _writeQueue.add(runnable);
        } else {
            runnable.run();
        }
    }
}
