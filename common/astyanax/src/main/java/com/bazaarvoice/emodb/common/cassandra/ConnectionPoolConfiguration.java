package com.bazaarvoice.emodb.common.cassandra;

import com.google.common.base.Optional;

/**
 * Interface for configuring the Cassandra cluster connection pool.  Note that depending on whether the Astyanax or
 * CQL driver is being used some of these configurations may not be applicable.
 */
public interface ConnectionPoolConfiguration {

    Optional<Integer> getInitialConnectionsPerHost();
    Optional<Integer> getMaxConnectionsPerHost();
    Optional<Integer> getCoreConnectionsPerHost();
    Optional<Integer> getSocketTimeout();
    Optional<Integer> getConnectTimeout();
    Optional<Integer> getMaxFailoverCount();
    Optional<Integer> getConnectionLimiterWindowSize();
    Optional<Integer> getConnectionLimiterMaxPendingCount();
    Optional<Integer> getMaxPendingConnectionsPerHost();
    Optional<Integer> getMaxBlockedThreadsPerHost();
    Optional<Integer> getMaxTimeoutCount();
    Optional<Integer> getTimeoutWindow();
    Optional<Integer> getRetrySuspendWindow();
    Optional<Integer> getRetryDelaySlice();
    Optional<Integer> getRetryMaxDelaySlice();
    Optional<Integer> getMaxTimeoutWhenExhausted();
}
