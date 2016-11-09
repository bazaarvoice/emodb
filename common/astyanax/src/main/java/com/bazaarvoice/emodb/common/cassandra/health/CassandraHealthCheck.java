package com.bazaarvoice.emodb.common.cassandra.health;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.dropwizard.guava.MoreSuppliers;
import com.codahale.metrics.health.HealthCheck;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ResultSet;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.cql.CqlStatementResult;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.netflix.astyanax.model.ConsistencyLevel.CL_ONE;

/**
 * Dropwizard health check for Cassandra.
 */
public class CassandraHealthCheck extends HealthCheck {
    private final CassandraKeyspace _keyspace;
    private final String _healthCheckCql;
    private final Supplier<Result> _resultCache;

    public CassandraHealthCheck(CassandraKeyspace keyspace, String healthCheckCql) {
        _keyspace = checkNotNull(keyspace, "keyspace");
        _healthCheckCql = checkNotNull(healthCheckCql, "healthCheckCql");

        // Rate limit health check calls to Cassandra.  Because there are typically several Cassandra connections used
        // by EmoDB randomize the cache refresh time to avoid requiring all Cassandra clusters to be queried on
        // a single health check in the event health checks are performed with high frequency.
        _resultCache = MoreSuppliers.memoizeWithRandomExpiration(this::pingAllUnchecked, 5, 10, TimeUnit.SECONDS);
    }

    @Override
    protected Result check() throws Exception {
        return _resultCache.get();
    }

    private Result pingAllUnchecked() {
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
            throw Throwables.propagate(t);
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
