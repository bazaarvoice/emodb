package com.bazaarvoice.emodb.sor.audit;

import com.bazaarvoice.emodb.sor.api.Audit;

/**
 * Defines the interface for writing audits to long term storage.
 */
public interface AuditWriter {

    void persist(String table, String key, Audit audit, long auditTime);
}