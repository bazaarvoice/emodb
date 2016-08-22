package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.History;

import java.util.List;

// A very thin wrapper for persisting writes to audit store, in case audits and compactions go to the same location,
// and only one write trip is required.
// In practice, we use it so that both delta history list and compaction are persisted in Cassandra with one batch mutation
public interface AuditBatchPersister {
    void commit(List<History> historyList, Object rowKey);
}
