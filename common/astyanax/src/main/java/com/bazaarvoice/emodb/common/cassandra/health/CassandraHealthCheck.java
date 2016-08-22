package com.bazaarvoice.emodb.common.cassandra.health;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.codahale.metrics.health.HealthCheck;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.ByteBufferSerializer;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.netflix.astyanax.model.ConsistencyLevel.CL_LOCAL_QUORUM;

/**
 * Dropwizard health check for Cassandra.
 */
public class CassandraHealthCheck extends HealthCheck {
    private final CassandraKeyspace _keyspace;
    private final ColumnFamily<ByteBuffer, ByteBuffer> _validationColumnFamily;
    private final Supplier<ByteBuffer> _keySupplier;
    private final Supplier<OperationResult<?>> _pingCache;

    public CassandraHealthCheck(CassandraKeyspace keyspace, String validationColumnFamily,
                                Supplier<ByteBuffer> keySupplier) {
        _keyspace = checkNotNull(keyspace, "keyspace");
        _validationColumnFamily = new ColumnFamily<>(
                checkNotNull(validationColumnFamily, "validationColumnFamily"),
                ByteBufferSerializer.get(), ByteBufferSerializer.get());
        _keySupplier = checkNotNull(keySupplier, "keySupplier");

        // Rate limit health check calls to Cassandra.
        _pingCache = Suppliers.memoizeWithExpiration(new Supplier<OperationResult<?>>() {
            @Override
            public OperationResult<?> get() {
                try {
                    return ping();
                } catch (Throwable t) {
                    throw Throwables.propagate(t);
                }
            }
        }, 5, TimeUnit.SECONDS);
    }

    public String getName() {
        return _keyspace.getName() + "-cassandra";
    }

    @Override
    protected Result check() throws Exception {
        OperationResult<?> result = _pingCache.get();
        return Result.healthy(
                result.getHost() +
                " " + result.getLatency(TimeUnit.MICROSECONDS) + "us" +
                (result.getAttemptsCount() != 1 ? ", " + result.getAttemptsCount() + " attempts" : ""));
    }

    private OperationResult<?> ping() throws Exception {
        // Get a random row to distribute queries among different servers in the ring.
        // Use quorum consistency to ensure a minimum # of nodes are alive.
        return _keyspace.prepareQuery(_validationColumnFamily, CL_LOCAL_QUORUM)
                .getKey(_keySupplier.get())
                .execute();
    }
}
