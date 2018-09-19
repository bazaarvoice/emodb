package com.bazaarvoice.emodb.sor.core;

/**
 * Primary interface for stopping writes as part of a graceful shutdown.
 * Once {@link #closeWrites()} has returned, all writes in progress have either finished or been interrupted,
 * and no further writes will be allowed.
 */
public interface DataWriteCloser {
    void closeWrites();
}
