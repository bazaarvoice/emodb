package com.bazaarvoice.emodb.common.cassandra.health;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.codahale.metrics.health.HealthCheck;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Stopwatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.cql.CqlStatementResult;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.netflix.astyanax.model.ConsistencyLevel.CL_ONE;
import static java.util.Objects.requireNonNull;

/**
 * Dropwizard health check for Cassandra.
 */
public class CassandraHealthCheck extends HealthCheck {
    private final CassandraKeyspace _keyspace;
    private final String _healthCheckCql;
    private final Clock _clock;
    private final ReentrantLock _lock = new ReentrantLock();
    private transient volatile Result _cachedResult;
    private transient volatile long _cacheExpirationTime = 0;
    private transient volatile long _cacheRefreshTimeoutTime = Long.MAX_VALUE;

    public CassandraHealthCheck(CassandraKeyspace keyspace, String healthCheckCql, Clock clock) {
        _keyspace = requireNonNull(keyspace, "keyspace");
        _healthCheckCql = requireNonNull(healthCheckCql, "healthCheckCql");
        _clock = requireNonNull(clock, "clock");

        // Optimistically return positive cached health check results until the first actual health check
        // returns a result
        _cachedResult = Result.healthy("Waiting for initial health check response");
    }

    @Override
    protected Result check() throws Exception {
        // Check if the cached result can be returned
        if (_clock.millis() < _cacheExpirationTime) {
            return _cachedResult;
        }

        // Attempt to get a lock on refreshing the result without blocking

        if (_lock.tryLock()) {
            try {
                // Lock acquired.  First verify the results still refreshing.
                if (_clock.millis() >= _cacheExpirationTime) {
                    // Set the cache refresh timeout to 30 seconds from now.  That way any concurrent health checks
                    // will return failure if the ping is taking longer than 30 seconds.
                    _cacheRefreshTimeoutTime = _clock.millis() + TimeUnit.SECONDS.toMillis(30);

                    // Perform the health check and cache the result
                    _cachedResult = pingAll();

                    // Set the cache expiration time to 5 seconds from now, forcing the cached result to be refreshed
                    // after that time.
                    _cacheExpirationTime = _clock.millis() + TimeUnit.SECONDS.toMillis(5);
                    // Reset the refresh timeout
                    _cacheRefreshTimeoutTime = Long.MAX_VALUE;
                }
            } finally {
                _lock.unlock();
            }
        } else {
            // Lock not acquired because another thread is currently updating and caching the heath check result.
            // If the current time is within the timeout time then continue returning the previously cached result.
            // Otherwise, the concurrent health check is taking an unreasonably long time and therefore Cassandra
            // should be considered unhealthy.

            if (_clock.millis() >= _cacheRefreshTimeoutTime) {
                return Result.unhealthy("Asynchronous health check update is taking too long");
            }
        }

        return _cachedResult;
    }

    private Result pingAll() {
        try {
            StringBuilder message = new StringBuilder();

            OperationResult<CqlStatementResult> astyanaxResult = pingAstyanax();
            message.append("Astyanax: ").append(astyanaxResult.getHost()).append(" ")
                    .append(astyanaxResult.getLatency(TimeUnit.MICROSECONDS)).append("us");

            if (astyanaxResult.getAttemptsCount() != 1) {
                message.append(", ").append(astyanaxResult.getAttemptsCount()).append(" attempts");
            }

            Stopwatch cqlTimer = Stopwatch.createStarted();
            ResultSet cqlResult = pingCql();
            long queryDurationMicros = cqlTimer.elapsed(TimeUnit.MICROSECONDS);

            Host host = cqlResult.getExecutionInfo().getQueriedHost();
            message.append(" | CQL: ").append(host).append(" ").append(queryDurationMicros).append("us");

            return Result.healthy(message.toString());
        } catch (Throwable t) {
            return Result.unhealthy(t);
        }
    }

    private OperationResult<CqlStatementResult> pingAstyanax() throws Exception {
        return _keyspace.getAstyanaxKeyspace().prepareCqlStatement()
                .withCql(_healthCheckCql)
                .withConsistencyLevel(CL_ONE)
                .execute();
    }

    private ResultSet pingCql() throws Exception {
        return _keyspace.getCqlSession().execute(_healthCheckCql);
    }
}
