package com.bazaarvoice.emodb.common.cassandra.metrics;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.inject.Inject;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;

/**
 * Implementation of {@link com.netflix.astyanax.connectionpool.ConnectionPoolMonitor} that takes advantage of the
 * existing counting logic in {@link CountingConnectionPoolMonitor} and exposes key metrics to the metric registry.
 */
public class MetricConnectionPoolMonitor extends CountingConnectionPoolMonitor {

    @Inject
    public MetricConnectionPoolMonitor(String poolName, MetricRegistry metricRegistry) {
        metricRegistry.register(
                MetricRegistry.name("bv.emodb.astyanax", poolName, "ConnectionPool", "open-connections"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return getNumOpenConnections();
                    }
                });

        metricRegistry.register(
                MetricRegistry.name("bv.emodb.astyanax", poolName, "ConnectionPool", "busy-connections"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return getNumBusyConnections();
                    }
                });

        metricRegistry.register(
                MetricRegistry.name("bv.emodb.astyanax", poolName, "ConnectionPool", "pool-exhausted-timeouts"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return getPoolExhaustedTimeoutCount();
                    }
                });

        metricRegistry.register(
                MetricRegistry.name("bv.emodb.astyanax", poolName, "ConnectionPool", "operation-timeouts"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return getOperationTimeoutCount();
                    }
                });

        metricRegistry.register(
                MetricRegistry.name("bv.emodb.astyanax", poolName, "ConnectionPool", "hosts"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return getHostCount();
                    }
                });

        metricRegistry.register(
                MetricRegistry.name("bv.emodb.astyanax", poolName, "ConnectionPool", "active-hosts"),
                new Gauge<Long>() {
                    @Override
                    public Long getValue() {
                        return getHostActiveCount();
                    }
                });
    }
}
