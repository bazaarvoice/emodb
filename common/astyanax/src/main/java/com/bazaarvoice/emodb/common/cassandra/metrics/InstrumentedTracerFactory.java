package com.bazaarvoice.emodb.common.cassandra.metrics;

import com.codahale.metrics.Clock;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.netflix.astyanax.CassandraOperationTracer;
import com.netflix.astyanax.CassandraOperationType;
import com.netflix.astyanax.KeyspaceTracerFactory;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;

import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;

/**
 * Tracks performance metrics for every Astyanax operation.
 */
public class InstrumentedTracerFactory implements KeyspaceTracerFactory {

    private final String _scope;
    private final MetricRegistry _registry;
    private final Clock _clock = Clock.defaultClock();
    private final LoadingCache<CassandraOperationType, Timer> _successTimers;
    private final LoadingCache<CassandraOperationType, Timer> _failureTimers;

    public InstrumentedTracerFactory(String scope, MetricRegistry metricRegistry) {
        _scope = scope;
        _registry = metricRegistry;
        _successTimers = CacheBuilder.newBuilder().build(new CacheLoader<CassandraOperationType, Timer>() {
            @Override
            public Timer load(CassandraOperationType opType) throws Exception {
                return _registry.timer(MetricRegistry.name("bv.emodb.astyanax", "Keyspace", getMetricName(opType, false), _scope));
            }
        });
        _failureTimers = CacheBuilder.newBuilder().build(new CacheLoader<CassandraOperationType, Timer>() {
            @Override
            public Timer load(CassandraOperationType opType) throws Exception {
                return _registry.timer(MetricRegistry.name("bv.emodb.astyanax", "Keyspace", getMetricName(opType, true), _scope));
            }
        });
    }

    private String getMetricName(CassandraOperationType operationType, boolean failure) {
        String name = operationType.name().toLowerCase().replace('_', '-');
        if (failure) {
            name += "-failed";
        }
        return name;
    }

    @Override
    public CassandraOperationTracer newTracer(final CassandraOperationType type) {
        return new CassandraOperationTracer() {
            private long _start;

            @Override
            public CassandraOperationTracer start() {
                checkState(_start == 0);  // Verify the tracer is used in a single threaded manner.
                _start = _clock.getTick();
                return this;
            }

            @Override
            public void success() {
                _successTimers.getUnchecked(type).update(_clock.getTick() - _start, TimeUnit.NANOSECONDS);
                _start = 0;
            }

            @Override
            public void failure(ConnectionException e) {
                _failureTimers.getUnchecked(type).update(_clock.getTick() - _start, TimeUnit.NANOSECONDS);
                _start = 0;
            }
        };
    }

    @Override
    public CassandraOperationTracer newTracer(CassandraOperationType type, ColumnFamily<?, ?> columnFamily) {
        // This class doesn't track column family-specific metrics.
        return newTracer(type);
    }
}
