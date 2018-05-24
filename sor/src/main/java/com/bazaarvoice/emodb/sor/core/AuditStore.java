package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.History;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;

/**
 * The interface for an Audit store for deltas that have been compacted away
 */
public interface AuditStore {
    /**
     * Returns the delta audits for a given row id
     */
    Iterator<Change> getDeltaAudits(String table, String rowId);

    /**
     * Puts delta audits in the audit store.
     */
    void putDeltaAudits(String table, String rowId, List<History> deltaAudits);

    /**
     * Puts delta audits in the audit store while accepting a {@link AuditBatchPersister}.
     * Use this call, if your AuditStore is in the same location as your regular storage,
     * and you would like the performance improvement of making one write call to handle audit and compaction writes.
     */
    void putDeltaAudits(Object rowId, List<History> deltaAudits, AuditBatchPersister auditBatchPersister);

    Duration getHistoryTtl();

    boolean isDeltaHistoryEnabled();
}

