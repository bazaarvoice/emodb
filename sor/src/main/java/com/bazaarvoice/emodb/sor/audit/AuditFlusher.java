package com.bazaarvoice.emodb.sor.audit;

/**
 * Primary interface for persisting audits as part of a graceful shutdown.
 * Once {@link #flushAndShutdown()} has returned, all audits have been sucessfully persisted and it is safe
 * to shutdown.
 */
public interface AuditFlusher {
    void flushAndShutdown();
}
