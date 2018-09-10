package com.bazaarvoice.emodb.sor.audit;


import com.bazaarvoice.emodb.sor.api.Audit;

/**
 * Audit writer implementation which discards all incoming audits.
 */
public class DiscardingAuditWriter implements AuditWriter, AuditFlusher {
    @Override
    public void persist(String table, String key, Audit audit, long auditTime) {
        // Discard
    }

    @Override
    public void flushAndShutdown() {
        // Do nothing
    }
}
