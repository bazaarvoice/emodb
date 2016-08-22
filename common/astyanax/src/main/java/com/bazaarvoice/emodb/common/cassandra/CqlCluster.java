package com.bazaarvoice.emodb.common.cassandra;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.Session;
import io.dropwizard.lifecycle.Managed;

import javax.annotation.Nullable;

/**
 * {@link CassandraCluster} implementation for connecting to Cassandra using the Datastax CQL driver.
 */
public class CqlCluster extends AbstractCassandraCluster<Session> implements Managed {

    private final Cluster _cluster;
    private final MetricRegistry _metricRegistry;
    private final String _metricName;

    public CqlCluster(Cluster cluster, String clusterName, String dataCenter, MetricRegistry metricRegistry,
                      @Nullable String metricName) {
        super(clusterName, dataCenter);
        _cluster = cluster;
        _metricRegistry = metricRegistry;
        _metricName = metricName;
    }

    @Override
    public void start() throws Exception {
        _cluster.init();
        registerMetrics();
    }

    @Override
    public void stop() throws Exception {
        _cluster.close();
    }

    @Override
    public Session connect(String keyspaceName) {
        return _cluster.connect(keyspaceName);
    }

    /**
     * Registers metrics for this cluster.  Ideally this would be done before the cluster is started so conflicting
     * metric names could be detected earlier, but since the CQL driver doesn't publish metrics until after it is
     * initialized the metrics cannot be registered until then.
     */
    private void registerMetrics() {
        if (_metricName == null) {
            // No metric name was provided; skip registration
            return;
        }

        Metrics metrics = _cluster.getMetrics();

        _metricRegistry.register(
                MetricRegistry.name("bv.emodb.cql", _metricName, "ConnectionPool", "connected-to-hosts"),
                metrics.getConnectedToHosts());

        _metricRegistry.register(
                MetricRegistry.name("bv.emodb.cql", _metricName, "ConnectionPool", "open-connections"),
                metrics.getOpenConnections());

        _metricRegistry.register(
                MetricRegistry.name("bv.emodb.cql", _metricName, "ConnectionPool", "connection-errors"),
                metrics.getErrorMetrics().getConnectionErrors());

        _metricRegistry.register(
                MetricRegistry.name("bv.emodb.cql", _metricName, "ConnectionPool", "read-timeouts"),
                metrics.getErrorMetrics().getReadTimeouts());

        _metricRegistry.register(
                MetricRegistry.name("bv.emodb.cql", _metricName, "ConnectionPool", "write-timeouts"),
                metrics.getErrorMetrics().getWriteTimeouts());

        _metricRegistry.register(
                MetricRegistry.name("bv.emodb.cql", _metricName, "ConnectionPool", "ignores"),
                metrics.getErrorMetrics().getIgnores());

        _metricRegistry.register(
                MetricRegistry.name("bv.emodb.cql", _metricName, "ConnectionPool", "unavailables"),
                metrics.getErrorMetrics().getUnavailables());

    }

}
