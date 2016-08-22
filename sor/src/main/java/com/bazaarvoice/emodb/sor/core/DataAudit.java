package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.delta.Delta;

import java.util.UUID;

/**
 * The minimal interface necessary to support the the data audit.
 */
public interface DataAudit {
    /**
     * Returns the changeId for the delta
     */
    UUID getChangeId();

    /**
     * Returns the Resolved object for a given changeId.
     */
    Resolved getResolved();

    /**
     * Returns the raw delta for this changeId
     */
    Delta getDelta();
}
