package com.bazaarvoice.emodb.sor.audit;

/**
 * Umbrella class for {@link AuditWriter} and {@link AuditFlusher}. This is interface has no functionality
 * and exists exclusively for binding semantics in {@link com.bazaarvoice.emodb.sor.DataStoreModule}
 */

public interface AuditStore extends AuditWriter, AuditFlusher {
}
