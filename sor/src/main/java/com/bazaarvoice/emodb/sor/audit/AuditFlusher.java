package com.bazaarvoice.emodb.sor.audit;

public interface AuditFlusher {
    void flushAndShutdown();
}
