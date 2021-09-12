package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.History;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

/**
 * The interface for a history store for deltas that have been compacted away
 */
public interface HistoryStore {
    /**
     * Returns the delta histories for a given row id
     */
    Iterator<Change> getDeltaHistories(String table, String rowId);

    /**
     * Puts delta histories in the histories store.
     */
    void putDeltaHistory(String table, String rowId, List<History> histories);

    /**
     * Puts delta histories in the history store while accepting a {@link HistoryBatchPersister}.
     * Use this call, if your HistoryStore is in the same location as your regular storage,
     * and you would like the performance improvement of making one write call to handle audit and compaction writes.
     */
    void putDeltaHistory(Object rowId, List<History> deltaAudits, HistoryBatchPersister historyBatchPersister);

    Duration getHistoryTtl();

    boolean isDeltaHistoryEnabled();
}

