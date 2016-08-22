package com.bazaarvoice.emodb.common.cassandra;

import com.bazaarvoice.emodb.common.cassandra.cqldriver.HintsPollerCQLSession;
import com.bazaarvoice.emodb.common.cassandra.cqldriver.SelectedHostLoadBalancingPolicy;
import com.bazaarvoice.emodb.common.cassandra.health.CassandraHealthCheck;
import com.bazaarvoice.emodb.common.cassandra.health.HealthCheckKeySupplier;
import com.bazaarvoice.emodb.common.dropwizard.guice.Global;
import com.bazaarvoice.emodb.common.dropwizard.healthcheck.HealthCheckRegistry;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Dropwizard factory for Cassandra per-keyspace connection pools.  Integrates with the Dropwizard lifecycle
 * events (start, stop) and adds a health check for the connection pool.
 */
public class CassandraFactory {
    private final Logger _log = LoggerFactory.getLogger(getClass());

    private final LifeCycleRegistry _lifeCycle;
    private final HealthCheckRegistry _healthChecks;
    private final CuratorFramework _curator;
    private final Supplier<ByteBuffer> _healthCheckKeySupplier;
    private final MetricRegistry _metricRegistry;

    @Inject
    public CassandraFactory(LifeCycleRegistry lifeCycle, HealthCheckRegistry healthChecks,
                            @Global CuratorFramework curator,
                            @HealthCheckKeySupplier Supplier<ByteBuffer> healthCheckKeySupplier,
                            MetricRegistry metricRegistry) {
        _lifeCycle = checkNotNull(lifeCycle, "lifeCycle");
        _curator = checkNotNull(curator, "zooKeeperConnection");
        _healthChecks = checkNotNull(healthChecks, "healthChecks");
        _healthCheckKeySupplier = checkNotNull(healthCheckKeySupplier, "healthCheckKeySupplier");
        _metricRegistry = metricRegistry;
    }

    public Map<String, CassandraKeyspace> build(CassandraConfiguration configuration) {
        // Cassandra nodes should register themselves in Zookeeper with cluster name as the key
        configuration.withZooKeeperHostDiscovery(_curator);

        AstyanaxCluster sharedAstyanaxCluster = null;
        CqlCluster sharedCqlCluster = null;

        Map<String, CassandraKeyspace> keyspaceMap = Maps.newHashMap();

        for (Map.Entry<String, KeyspaceConfiguration> entry : configuration.getKeyspaces().entrySet()) {
            String keyspaceName = entry.getKey();
            KeyspaceConfiguration keyspaceConfig = entry.getValue();
            String healthCheckColumnFamily = keyspaceConfig.getHealthCheckColumnFamily();
            AstyanaxCluster astyanaxCluster;
            CqlCluster cqlCluster;

            if (keyspaceConfig.useSharedConnectionPool()) {
                _log.info("Connection to keyspace {} will be created using the shared connection to cluster \"{}\"",
                        keyspaceName, configuration.getCluster());

                if (sharedAstyanaxCluster == null) {
                    // Shared connection pool doesn't exist yet so create it now
                    sharedAstyanaxCluster = configuration.astyanax().metricRegistry(_metricRegistry).cluster();
                    sharedCqlCluster = configuration.cql().metricRegistry(_metricRegistry).cluster();
                    _lifeCycle.manage(sharedAstyanaxCluster);
                    _lifeCycle.manage(sharedCqlCluster);
                }
                astyanaxCluster = sharedAstyanaxCluster;
                cqlCluster = sharedCqlCluster;
            } else {
                _log.info("Connection to keyspace {} will be created using a private connection to cluster \"{}\"",
                        keyspaceName, configuration.getCluster());

                astyanaxCluster = configuration.astyanax()
                        .keyspace(keyspaceName)
                        .metricRegistry(_metricRegistry)
                        .cluster();

                cqlCluster = configuration.cql()
                        .keyspace(keyspaceName)
                        .metricRegistry(_metricRegistry)
                        .cluster();

                _lifeCycle.manage(astyanaxCluster);
                _lifeCycle.manage(cqlCluster);
            }

            CassandraKeyspace keyspace = new CassandraKeyspace(_lifeCycle, keyspaceName, astyanaxCluster, cqlCluster);
            CassandraHealthCheck healthCheck = newHealthCheck(keyspace, healthCheckColumnFamily);
            _healthChecks.addHealthCheck(healthCheck.getName(), healthCheck);

            keyspaceMap.put(keyspaceName, keyspace);

        }
        return keyspaceMap;
    }

    public HintsPollerCQLSession cqlSessionForHintsPoller(CassandraConfiguration configuration) {
        // Nodes can register themselves in ZooKeeper to help figure out which hosts are in this data center.
        configuration.withZooKeeperHostDiscovery(_curator);

        // Hints Poller only need 1 connection per host since all this is used for is single-threaded polling of the hints table.
        // We could also have a new property in the CassandraConfiguration yaml's just for the HintsPoller sake.
        CqlCluster cqlCluster = configuration.cql()
                .metricRegistry(_metricRegistry)
                .disableClusterMetrics()
                .maxConnectionsPerHost(1)
                .coreConnectionsPerHost(1)
                .loadBalancingPolicy(new SelectedHostLoadBalancingPolicy())
                .retryPolicy(FallthroughRetryPolicy.INSTANCE)
                .cluster();

        _lifeCycle.manage(cqlCluster);

        return new HintsPollerCQLSession(_lifeCycle, cqlCluster);
    }

    protected CassandraHealthCheck newHealthCheck(CassandraKeyspace keyspace, String healthCheckColumnFamily) {
        return new CassandraHealthCheck(keyspace, healthCheckColumnFamily, _healthCheckKeySupplier);
    }
}
