package com.bazaarvoice.emodb.common.cassandra.health;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.dropwizard.guava.MoreSuppliers;
import com.codahale.metrics.health.HealthCheck;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.serializers.ByteBufferSerializer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.netflix.astyanax.model.ConsistencyLevel.CL_LOCAL_QUORUM;

/**
 * Dropwizard health check for Cassandra.
 */
public class CassandraHealthCheck extends HealthCheck {
    private final CassandraKeyspace _keyspace;
    private final ColumnFamily<ByteBuffer, ByteBuffer> _validationColumnFamily;
    private final Supplier<ByteBuffer> _keySupplier;
    private final Supplier<Result> _resultCache;
    private final Supplier<PreparedStatement> _cqlStatement;

    public CassandraHealthCheck(CassandraKeyspace keyspace, String validationColumnFamily,
                                Supplier<ByteBuffer> keySupplier) {
        _keyspace = checkNotNull(keyspace, "keyspace");
        _validationColumnFamily = new ColumnFamily<>(
                checkNotNull(validationColumnFamily, "validationColumnFamily"),
                ByteBufferSerializer.get(), ByteBufferSerializer.get());
        _keySupplier = checkNotNull(keySupplier, "keySupplier");

        _cqlStatement = Suppliers.memoize(this::prepareCqlStatement);

        // Rate limit health check calls to Cassandra.  Because there are typically several Cassandra connections used
        // by EmoDB randomize the cache refresh time to avoid requiring all Cassandra clusters to be queried on
        // a single health check in the event health checks are performed with high frequency.
        _resultCache = MoreSuppliers.memoizeWithRandomExpiration(this::pingAllUnchecked, 5, 10, TimeUnit.SECONDS);
    }

    public String getName() {
        return _keyspace.getName() + "-cassandra";
    }

    @Override
    protected Result check() throws Exception {
        return _resultCache.get();
    }

    private Result pingAllUnchecked() {
        try {
            // Get a random row to distribute queries among different servers in the ring.
            ByteBuffer key = _keySupplier.get();
            StringBuilder message = new StringBuilder();

            ListenableFuture<OperationResult<ColumnList<ByteBuffer>>> astyanaxResultFuture = pingAstyanax(key);

            // The Astyanax driver includes timing metrics in the result.  To get the same from the CQL driver
            // requires query tracing.  However, this adds unnecessary overhead to the query.  Additionally, it can
            // cause the health check to take a relatively long time waiting for trace fetch retries
            // if the driver eventually throws a TraceRetrievalException.  So use a local timer to get a coarse
            // estimate of the query time instead.
            Stopwatch cqlTimer = Stopwatch.createStarted();
            ResultSetFuture cqlResultFuture = pingCql(key);
            cqlResultFuture.addListener(cqlTimer::stop, MoreExecutors.sameThreadExecutor());

            OperationResult<ColumnList<ByteBuffer>> astyanaxResult = Futures.getUnchecked(astyanaxResultFuture);
            message.append("Astyanax: ").append(astyanaxResult.getHost()).append(" ")
                    .append(astyanaxResult.getLatency(TimeUnit.MICROSECONDS)).append("us");

            if (astyanaxResult.getAttemptsCount() != 1) {
                message.append(", ").append(astyanaxResult.getAttemptsCount()).append(" attempts");
            }

            ResultSet cqlResult = cqlResultFuture.getUninterruptibly();
            long queryDurationMicros = cqlTimer.elapsed(TimeUnit.MICROSECONDS);

            Host host = cqlResult.getExecutionInfo().getQueriedHost();
            message.append(" | CQL: ").append(host).append(" ").append(queryDurationMicros).append("us");

            return Result.healthy(message.toString());
        } catch (Throwable t) {
            throw Throwables.propagate(t);
        }
    }

    private ListenableFuture<OperationResult<ColumnList<ByteBuffer>>> pingAstyanax(ByteBuffer key) throws Exception {
        // Use quorum consistency to ensure a minimum # of nodes are alive.
        return _keyspace.prepareQuery(_validationColumnFamily, CL_LOCAL_QUORUM)
                .getKey(key)
                .executeAsync();
    }

    private PreparedStatement prepareCqlStatement() {
        List<ColumnMetadata> metadata = _keyspace.getKeyspaceMetadata().getTable(_validationColumnFamily.getName()).getPartitionKey();
        String query = "select " +
                Joiner.on(", ").join(metadata.stream().map(ColumnMetadata::getName).collect(Collectors.toList())) +
                " from " + _validationColumnFamily.getName() +
                " where " +
                Joiner.on(" and ").join(metadata.stream().map((md) -> "token(" + md.getName() + ")=?").collect(Collectors.toList())) +
                " limit 1";

        return _keyspace.getCqlSession().prepare(query).setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM);
    }

    private ResultSetFuture pingCql(ByteBuffer key) throws Exception {
        PreparedStatement preparedStatement = _cqlStatement.get();
        BoundStatement statement = preparedStatement.bind();
        for (int i=0; i < preparedStatement.getVariables().size(); i++) {
            statement.setBytesUnsafe(i, key);
        }

        return _keyspace.getCqlSession().executeAsync(statement);
    }
}
