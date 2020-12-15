package com.bazaarvoice.emodb.common.cassandra;

import com.bazaarvoice.emodb.common.cassandra.metrics.InstrumentedTracerFactory;
import com.bazaarvoice.emodb.common.cassandra.metrics.MetricConnectionPoolMonitor;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.ostrich.HostDiscovery;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.discovery.zookeeper.ZooKeeperHostDiscovery;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.RetryPolicy;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.connectionpool.LatencyScoreStrategy;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.connectionpool.impl.EmaLatencyScoreStrategyImpl;
import com.netflix.astyanax.connectionpool.impl.SimpleAuthenticationCredentials;
import com.netflix.astyanax.connectionpool.impl.Slf4jConnectionPoolMonitorImpl;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.shallows.EmptyLatencyScoreStrategyImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import io.dropwizard.util.Size;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

/**
 * Dropwizard YAML-friendly configuration settings for a Cassandra keyspace and/or cluster
 * using the Astyanax client library.
 */
public class CassandraConfiguration implements ConnectionPoolConfiguration {

    private final Logger _log = LoggerFactory.getLogger(getClass());

    /**
     * Name of the cluster.  This should correspond to the server's configured cluster name.
     */
    @NotNull
    private String _cluster;

    private String _clusterMetric;

    @NotNull
    private Map<String, KeyspaceConfiguration> _keyspaces;

    private CassandraPartitioner _partitioner;

    private String _dataCenter;

    @NotNull
    private CassandraHealthCheckConfiguration _healthCheck = new CassandraHealthCheckConfiguration();

    /**
     * Log changes to the connection pool host list?
     */
    private boolean _verboseHostLogging;

    /**
     * Use BV SOA (Ostrich) host discovery to find Cassandra nodes registered under a specified service name.
     */
    private String _zooKeeperServiceName;
    private transient CuratorFramework _curator;

    //
    // YAML-friendly ConnectionPoolConfigurationImpl properties
    //
    private String _seeds;
    private boolean _hostDiscoveryPerformed = false;

    private Optional<Integer> _initialConnectionsPerHost = Optional.absent();
    private Optional<Integer> _maxConnectionsPerHost = Optional.absent();
    private Optional<Integer> _coreConnectionsPerHost = Optional.absent();
    private int _thriftPort = ConnectionPoolConfigurationImpl.DEFAULT_PORT;
    private int _cqlPort = ProtocolOptions.DEFAULT_PORT;
    private Optional<Integer> _socketTimeout = Optional.absent();
    private Optional<Integer> _connectTimeout = Optional.absent();
    private Optional<Integer> _maxFailoverCount = Optional.absent();
    // not used
    private boolean _latencyAware;
    private int _latencyAwareWindowSize = ConnectionPoolConfigurationImpl.DEFAULT_LATENCY_AWARE_WINDOW_SIZE;
    //    private float _latencyAwareSentinelCompare = ConnectionPoolConfigurationImpl.DEFAULT_LATENCY_AWARE_SENTINEL_COMPARE;
    private float _latencyAwareBadnessThreshold = ConnectionPoolConfigurationImpl.DEFAULT_LATENCY_AWARE_BADNESS_THRESHOLD;
    private int _latencyAwareUpdateInterval = ConnectionPoolConfigurationImpl.DEFAULT_LATENCY_AWARE_UPDATE_INTERVAL;
    private int _latencyAwareResetInterval = ConnectionPoolConfigurationImpl.DEFAULT_LATENCY_AWARE_RESET_INTERVAL;
    private Optional<Integer> _connectionLimiterWindowSize = Optional.absent();
    private Optional<Integer> _connectionLimiterMaxPendingCount = Optional.absent();
    private Optional<Integer> _maxPendingConnectionsPerHost = Optional.absent();
    private Optional<Integer> _maxBlockedThreadsPerHost = Optional.absent();
    private Optional<Integer> _maxTimeoutCount = Optional.absent();
    private Optional<Integer> _timeoutWindow = Optional.absent();
    // ExponentialRetryBackoffStrategy configuration
    private Optional<Integer> _retrySuspendWindow = Optional.absent();
    private Optional<Integer> _retryDelaySlice = Optional.absent();
    private Optional<Integer> _retryMaxDelaySlice = Optional.absent();
    // BagOfConnectionsConnectionPoolImpl configuration
    //    private int _maximumConnections = ConnectionPoolConfigurationImpl.DEFAULT_MAX_CONNS;
    //    private int _maxOperationsPerConnection = ConnectionPoolConfigurationImpl.DEFAULT_MAX_OPERATIONS_PER_CONNECTION;
    private Optional<Integer> _maxTimeoutWhenExhausted = Optional.absent();
    private SimpleAuthenticationCredentials _authenticationCredentials;
    private Optional<Size> _maxThriftFrameSize = Optional.absent();

    //
    // Post-configuration methods
    //

    public CassandraConfiguration withZooKeeperHostDiscovery(CuratorFramework curator) {
        if (_zooKeeperServiceName != null) {
            if (_curator != null && !_curator.equals(curator)) {
                throw new IllegalStateException("Curator is already set to a different value.");
            }
            if (_curator == null) {
                _curator = curator;
            }
        }
        return this;
    }

    public Astyanax astyanax() {
        return new Astyanax();
    }

    /**
     * Configuration class that acts as a builder for an Astyanax cluster connection.
     */
    public class Astyanax {
        private String _keyspace = null;
        private MetricRegistry _metricRegistry;
        private boolean _disableClusterMetrics = false;

        private Astyanax() {
            // empty
        }

        public Astyanax keyspace(String keyspace) {
            _keyspace = keyspace;
            return this;
        }

        public Astyanax metricRegistry(MetricRegistry metricRegistry) {
            _metricRegistry = metricRegistry;
            return this;
        }

        public Astyanax disableClusterMetrics() {
            _disableClusterMetrics = true;
            return this;
        }

        public AstyanaxCluster cluster() {
            requireNonNull(_cluster, "cluster");
            String metricName;
            ConnectionPoolConfiguration poolConfig;

            if (_keyspace == null) {
                // Use the shared pool configuration
                metricName = ofNullable(_clusterMetric).orElse(_cluster);
                poolConfig = CassandraConfiguration.this;
            } else {
                // Use the configuration specifically for this keyspace
                KeyspaceConfiguration keyspaceConfig = requireNonNull(_keyspaces.get(_keyspace), "keyspaceConfig");
                metricName = ofNullable(keyspaceConfig.getKeyspaceMetric()).orElse(_keyspace);
                poolConfig = keyspaceConfig;
            }

            AstyanaxContext.Builder builder = newAstyanaxBuilder(_cluster, poolConfig, _metricRegistry)
                    .forCluster(_cluster);

            if (!_disableClusterMetrics) {
                builder = builder
                        .withTracerFactory(new InstrumentedTracerFactory(metricName, _metricRegistry))
                        .withConnectionPoolMonitor(new MetricConnectionPoolMonitor(metricName, _metricRegistry));
            }

            AstyanaxContext<Cluster> astyanaxContext = builder.buildCluster(ThriftFamilyFactory.getInstance());

            return new AstyanaxCluster(astyanaxContext, _cluster, _dataCenter);
        }
    }

    private AstyanaxContext.Builder newAstyanaxBuilder(String name, ConnectionPoolConfiguration poolConfig,
                                                       MetricRegistry metricRegistry) {
        performHostDiscovery(metricRegistry);

        LatencyScoreStrategy latencyScoreStrategy = _latencyAware ?
                new EmaLatencyScoreStrategyImpl(_latencyAwareWindowSize) :
                new EmptyLatencyScoreStrategyImpl();

        ConnectionPoolConfigurationImpl poolConfiguration = new ConnectionPoolConfigurationImpl(name)
                .setLocalDatacenter(_dataCenter)
                .setSeeds(_seeds)
                .setPartitioner(_partitioner.newAstyanaxPartitioner())
                .setInitConnsPerHost(poolConfig.getInitialConnectionsPerHost().or(getInitialConnectionsPerHost()).or(ConnectionPoolConfigurationImpl.DEFAULT_INIT_PER_PARTITION))
                .setMaxConnsPerHost(poolConfig.getMaxConnectionsPerHost().or(getMaxConnectionsPerHost()).or(ConnectionPoolConfigurationImpl.DEFAULT_MAX_ACTIVE_PER_PARTITION))
                .setPort(_thriftPort)
                .setSocketTimeout(poolConfig.getSocketTimeout().or(getSocketTimeout()).or(ConnectionPoolConfigurationImpl.DEFAULT_SOCKET_TIMEOUT))
                .setConnectTimeout(poolConfig.getConnectTimeout().or(getConnectTimeout()).or(ConnectionPoolConfigurationImpl.DEFAULT_CONNECT_TIMEOUT))
                .setMaxFailoverCount(poolConfig.getMaxFailoverCount().or(getMaxFailoverCount()).or(ConnectionPoolConfigurationImpl.DEFAULT_FAILOVER_COUNT))
                .setConnectionLimiterWindowSize(poolConfig.getConnectionLimiterWindowSize().or(getConnectionLimiterWindowSize()).or(ConnectionPoolConfigurationImpl.DEFAULT_CONNECTION_LIMITER_WINDOW_SIZE))
                .setConnectionLimiterMaxPendingCount(poolConfig.getConnectionLimiterMaxPendingCount().or(getConnectionLimiterMaxPendingCount()).or(ConnectionPoolConfigurationImpl.DEFAULT_CONNECTION_LIMITER_MAX_PENDING_COUNT))
                .setMaxPendingConnectionsPerHost(poolConfig.getMaxPendingConnectionsPerHost().or(getMaxPendingConnectionsPerHost()).or(ConnectionPoolConfigurationImpl.DEFAULT_MAX_PENDING_CONNECTIONS_PER_HOST))
                .setMaxBlockedThreadsPerHost(poolConfig.getMaxBlockedThreadsPerHost().or(getMaxBlockedThreadsPerHost()).or(ConnectionPoolConfigurationImpl.DEFAULT_MAX_BLOCKED_THREADS_PER_HOST))
                .setMaxTimeoutCount(poolConfig.getMaxTimeoutCount().or(getMaxTimeoutCount()).or(ConnectionPoolConfigurationImpl.DEFAULT_MAX_TIMEOUT_COUNT))
                .setTimeoutWindow(poolConfig.getTimeoutWindow().or(getTimeoutWindow()).or(ConnectionPoolConfigurationImpl.DEFAULT_TIMEOUT_WINDOW))
                .setRetrySuspendWindow(poolConfig.getRetrySuspendWindow().or(getRetrySuspendWindow()).or(ConnectionPoolConfigurationImpl.DEFAULT_RETRY_SUSPEND_WINDOW))
                .setRetryDelaySlice(poolConfig.getRetryDelaySlice().or(getRetryDelaySlice()).or(ConnectionPoolConfigurationImpl.DEFAULT_RETRY_DELAY_SLICE))
                .setRetryMaxDelaySlice(poolConfig.getRetryMaxDelaySlice().or(getRetryMaxDelaySlice()).or(ConnectionPoolConfigurationImpl.DEFAULT_RETRY_MAX_DELAY_SLICE))
                .setMaxTimeoutWhenExhausted(poolConfig.getMaxTimeoutWhenExhausted().or(getMaxTimeoutWhenExhausted()).or(ConnectionPoolConfigurationImpl.DEFAULT_MAX_TIME_WHEN_EXHAUSTED))
                .setAuthenticationCredentials(_authenticationCredentials)
                .setLatencyScoreStrategy(latencyScoreStrategy);

        CountingConnectionPoolMonitor poolMonitor = _verboseHostLogging ?
                new Slf4jConnectionPoolMonitorImpl() :
                new CountingConnectionPoolMonitor();

        AstyanaxConfigurationImpl asConfig = new AstyanaxConfigurationImpl()
                .setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE)
                .setDiscoveryType(NodeDiscoveryType.TOKEN_AWARE)
                .setTargetCassandraVersion("1.2");

        if (_maxThriftFrameSize.isPresent()) {
            asConfig.setMaxThriftSize((int) _maxThriftFrameSize.get().toBytes());
        }

        return new AstyanaxContext.Builder()
                .withAstyanaxConfiguration(asConfig)
                .withConnectionPoolConfiguration(poolConfiguration)
                .withConnectionPoolMonitor(poolMonitor);
    }

    public Cql cql() {
        return new Cql();
    }

    /**
     * Configuration class that acts as a builder for a CQL driver cluster connection.
     */
    public class Cql {
        private String _keyspace;
        private MetricRegistry _metricRegistry;
        private boolean _disableClusterMetrics = false;
        private LoadBalancingPolicy _loadBalancingPolicy;
        private RetryPolicy _retryPolicy;
        private Optional<Integer> _maxConnectionsPerHost = Optional.absent();
        private Optional<Integer> _coreConnectionsPerHost = Optional.absent();

        private Cql() {
            // empty
        }

        public Cql keyspace(String keyspace) {
            _keyspace = keyspace;
            return this;
        }

        public Cql metricRegistry(MetricRegistry metricRegistry) {
            _metricRegistry = metricRegistry;
            return this;
        }

        public Cql disableClusterMetrics() {
            _disableClusterMetrics = true;
            return this;
        }

        public Cql loadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
            _loadBalancingPolicy = loadBalancingPolicy;
            return this;
        }

        public Cql retryPolicy(RetryPolicy retryPolicy) {
            _retryPolicy = retryPolicy;
            return this;
        }

        public Cql maxConnectionsPerHost(int maxConnectionsPerHost) {
            _maxConnectionsPerHost = Optional.of(maxConnectionsPerHost);
            return this;
        }

        public Cql coreConnectionsPerHost(int coreConnectionsPerHost) {
            _coreConnectionsPerHost = Optional.of(coreConnectionsPerHost);
            return this;
        }

        public CqlCluster cluster() {
            requireNonNull(_cluster, "cluster");
            String metricName;
            FilterConnectionPoolConfiguration poolConfig;

            if (_keyspace == null) {
                // Use the shared pool configuration
                metricName = ofNullable(_clusterMetric).orElse(_cluster);
                poolConfig = new FilterConnectionPoolConfiguration(CassandraConfiguration.this);
            } else {
                // Use the configuration specifically for this keyspace
                KeyspaceConfiguration keyspaceConfig = requireNonNull(_keyspaces.get(_keyspace), "keyspaceConfig");
                metricName = ofNullable(keyspaceConfig.getKeyspaceMetric()).orElse(_keyspace);
                poolConfig = new FilterConnectionPoolConfiguration(keyspaceConfig);
            }

            if (_maxConnectionsPerHost.isPresent()) {
                poolConfig.setMaxConnectionsPerHost(_maxConnectionsPerHost.get());
            }
            if (_coreConnectionsPerHost.isPresent()) {
                poolConfig.setCoreConnectionsPerHost(_coreConnectionsPerHost.get());
            }

            // Set any unset policies to the default
            _loadBalancingPolicy = ofNullable(_loadBalancingPolicy).orElse(Policies.defaultLoadBalancingPolicy());
            _retryPolicy = ofNullable(_retryPolicy).orElse(Policies.defaultRetryPolicy());

            com.datastax.driver.core.Cluster cluster = newCqlDriverBuilder(poolConfig, _metricRegistry)
                    .withClusterName(_cluster)
                    .withLoadBalancingPolicy(_loadBalancingPolicy)
                    .withRetryPolicy(_retryPolicy)
                    .build();

            if (_disableClusterMetrics) {
                // Setting the metrics name to null results in no metrics for this cluster being published
                metricName = null;
            }

            return new CqlCluster(cluster, _cluster, _dataCenter, _metricRegistry, metricName);
        }
    }

    private com.datastax.driver.core.Cluster.Builder newCqlDriverBuilder(ConnectionPoolConfiguration poolConfig,
                                                                         MetricRegistry metricRegistry) {
        performHostDiscovery(metricRegistry);

        String[] seeds = _seeds.split(",");
        List<String> contactPoints = new ArrayList<>(seeds.length);

        // Each seed may be a host name or a host name and port (e.g.; "1.2.3.4" or "1.2.3.4:9160").  These need
        // to be converted into host names only.
        for (String seed : seeds) {
            HostAndPort hostAndPort = HostAndPort.fromString(seed);
            seed = hostAndPort.getHostText();
            if (hostAndPort.hasPort()) {
                if (hostAndPort.getPort() == _thriftPort) {
                    _log.debug("Seed {} found using RPC port; swapping for native port {}", seed, _cqlPort);
                } else if (hostAndPort.getPort() != _cqlPort) {
                    throw new IllegalArgumentException(String.format(
                            "Seed %s found with invalid port %s.  The port must match either the RPC (thrift) port %s " +
                            "or the native (CQL) port %s", seed, hostAndPort.getPort(), _thriftPort, _cqlPort));
                }
            }

            contactPoints.add(seed);
        }

        PoolingOptions poolingOptions = new PoolingOptions();
        if (poolConfig.getMaxConnectionsPerHost().or(getMaxConnectionsPerHost()).isPresent()) {
            poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, poolConfig.getMaxConnectionsPerHost().or(getMaxConnectionsPerHost()).get());
        }
        if (poolConfig.getCoreConnectionsPerHost().or(getCoreConnectionsPerHost()).isPresent()) {
            poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, poolConfig.getCoreConnectionsPerHost().or(getCoreConnectionsPerHost()).get());
        }

        SocketOptions socketOptions = new SocketOptions();
        if (poolConfig.getConnectTimeout().or(getConnectTimeout()).isPresent()) {
            socketOptions.setConnectTimeoutMillis(poolConfig.getConnectTimeout().or(getConnectTimeout()).get());
        }
        if (poolConfig.getSocketTimeout().or(getSocketTimeout()).isPresent()) {
            socketOptions.setReadTimeoutMillis(poolConfig.getSocketTimeout().or(getSocketTimeout()).get());
        }

        AuthProvider authProvider = _authenticationCredentials != null
                ? new PlainTextAuthProvider(_authenticationCredentials.getUsername(), _authenticationCredentials.getPassword())
                : AuthProvider.NONE;

        return com.datastax.driver.core.Cluster.builder()
                .addContactPoints(contactPoints.toArray(new String[contactPoints.size()]))
                .withPort(_cqlPort)
                .withPoolingOptions(poolingOptions)
                .withSocketOptions(socketOptions)
                .withRetryPolicy(Policies.defaultRetryPolicy())
                .withAuthProvider(authProvider);
    }

    /**
     * Discover Cassandra seeds and partitioner, if not statically configured.
     */
    synchronized public void performHostDiscovery(MetricRegistry metricRegistry) {
        // Host discovery is not idempotent; only take action if it hasn't been performed.
        if (_hostDiscoveryPerformed) {
            return;
        }

        Iterable<String> hosts = null;

        // Statically configured list of seeds.
        if (_seeds != null) {
            hosts = Splitter.on(',').trimResults().split(_seeds);
        }

        // Perform ZooKeeper discovery to find the seeds.
        if (_zooKeeperServiceName != null) {
            if (hosts != null) {
                throw new IllegalStateException("Too many host discovery mechanisms configured.");
            }

            if (_curator == null) {
                throw new IllegalStateException("ZooKeeper host discovery is configured but withZooKeeperHostDiscovery() was not called.");
            }

            try (HostDiscovery hostDiscovery = new ZooKeeperHostDiscovery(_curator, _zooKeeperServiceName, metricRegistry)) {
                List<String> hostList = new ArrayList<>();
                for (ServiceEndPoint endPoint : hostDiscovery.getHosts()) {
                    // The host:port is in the end point ID
                    hostList.add(endPoint.getId());

                    // The partitioner class name is in the json-encoded end point payload
                    if (_partitioner == null && endPoint.getPayload() != null) {
                        JsonNode payload = JsonHelper.fromJson(endPoint.getPayload(), JsonNode.class);
                        String partitioner = payload.path("partitioner").textValue();
                        if (partitioner != null) {
                            _partitioner = CassandraPartitioner.fromClass(partitioner);
                        }
                    }
                }
                hosts = hostList;
            } catch (IOException ex) {
                // suppress any IOExceptions that might result from closing our ZooKeeperHostDiscovery
            }
        }

        if (hosts == null) {
            throw new IllegalStateException("No Cassandra host discovery mechanisms are configured.");
        }

        if (Iterables.isEmpty(hosts)) {
            throw new IllegalStateException("Unable to discover any Cassandra seed instances.");
        }

        if (_partitioner == null) {
            throw new IllegalStateException("Cassandra partitioner not configured or discoverable.");
        }
        _seeds = Joiner.on(',').join(hosts);
        _hostDiscoveryPerformed = true;
    }

    //
    // Getter and Setter methods
    //

    public String getCluster() {
        return _cluster;
    }

    public CassandraConfiguration setCluster(String cluster) {
        _cluster = cluster;
        return this;
    }

    public String getClusterMetric() {
        return _clusterMetric;
    }

    public CassandraConfiguration setClusterMetric(String clusterMetric) {
        _clusterMetric = clusterMetric;
        return this;
    }

    public Map<String, KeyspaceConfiguration> getKeyspaces() {
        return _keyspaces;
    }

    public CassandraConfiguration setKeyspaces(Map<String, KeyspaceConfiguration> keyspaces) {
        _keyspaces = Collections.unmodifiableMap(new HashMap<>(keyspaces));
        return this;
    }

    public CassandraPartitioner getPartitioner() {
        return _partitioner;
    }

    @JsonProperty("partitioner")
    public CassandraConfiguration setPartitioner(String partitioner) {
        return setPartitioner(CassandraPartitioner.valueOf(partitioner.toUpperCase()));
    }

    @JsonIgnore
    public CassandraConfiguration setPartitioner(CassandraPartitioner partitioner) {
        _partitioner = partitioner;
        return this;
    }

    public String getDataCenter() {
        return _dataCenter;
    }

    public CassandraConfiguration setDataCenter(String dataCenter) {
        _dataCenter = dataCenter;
        return this;
    }

    public CassandraHealthCheckConfiguration getHealthCheck() {
        return _healthCheck;
    }

    public CassandraConfiguration setHealthCheck(CassandraHealthCheckConfiguration healthCheck) {
        _healthCheck = healthCheck;
        return this;
    }

    public boolean isVerboseHostLogging() {
        return _verboseHostLogging;
    }

    public CassandraConfiguration setVerboseHostLogging(boolean verboseHostLogging) {
        _verboseHostLogging = verboseHostLogging;
        return this;
    }

    public String getSeeds() {
        return _seeds;
    }

    public CassandraConfiguration setSeeds(String seeds) {
        _seeds = seeds;
        return this;
    }

    public String getZooKeeperServiceName() {
        return _zooKeeperServiceName;
    }

    public CassandraConfiguration setZooKeeperServiceName(String zooKeeperServiceName) {
        _zooKeeperServiceName = zooKeeperServiceName;
        return this;
    }

    @Override
    public Optional<Integer> getInitialConnectionsPerHost() {
        return _initialConnectionsPerHost;
    }

    public CassandraConfiguration setInitialConnectionsPerHost(Optional<Integer> initialConnectionsPerHost) {
        _initialConnectionsPerHost = initialConnectionsPerHost;
        return this;
    }

    @Override
    public Optional<Integer> getMaxConnectionsPerHost() {
        return _maxConnectionsPerHost;
    }

    public CassandraConfiguration setMaxConnectionsPerHost(Optional<Integer> maxConnectionsPerHost) {
        _maxConnectionsPerHost = maxConnectionsPerHost;
        return this;
    }

    @Override
    public Optional<Integer> getCoreConnectionsPerHost() {
        return _coreConnectionsPerHost;
    }

    public void setCoreConnectionsPerHost(Optional<Integer> coreConnectionsPerHost) {
        _coreConnectionsPerHost = coreConnectionsPerHost;
    }

    public int getThriftPort() {
        return _thriftPort;
    }

    public CassandraConfiguration setThriftPort(int thriftPort) {
        _thriftPort = thriftPort;
        return this;
    }

    public int getCqlPort() {
        return _cqlPort;
    }

    public CassandraConfiguration setCqlPort(int cqlPort) {
        _cqlPort = cqlPort;
        return this;
    }

    @Override
    public Optional<Integer> getSocketTimeout() {
        return _socketTimeout;
    }

    public CassandraConfiguration setSocketTimeout(Optional<Integer> socketTimeout) {
        _socketTimeout = socketTimeout;
        return this;
    }

    @Override
    public Optional<Integer> getConnectTimeout() {
        return _connectTimeout;
    }

    public CassandraConfiguration setConnectTimeout(Optional<Integer> connectTimeout) {
        _connectTimeout = connectTimeout;
        return this;
    }

    @Override
    public Optional<Integer> getMaxFailoverCount() {
        return _maxFailoverCount;
    }

    public CassandraConfiguration setMaxFailoverCount(Optional<Integer> maxFailoverCount) {
        _maxFailoverCount = maxFailoverCount;
        return this;
    }

    public boolean isLatencyAware() {
        return _latencyAware;
    }

    public CassandraConfiguration setLatencyAware(boolean latencyAware) {
        _latencyAware = latencyAware;
        return this;
    }

    public int getLatencyAwareUpdateInterval() {
        return _latencyAwareUpdateInterval;
    }

    public CassandraConfiguration setLatencyAwareUpdateInterval(int latencyAwareUpdateInterval) {
        _latencyAwareUpdateInterval = latencyAwareUpdateInterval;
        return this;
    }

    public int getLatencyAwareResetInterval() {
        return _latencyAwareResetInterval;
    }

    public CassandraConfiguration setLatencyAwareResetInterval(int latencyAwareResetInterval) {
        _latencyAwareResetInterval = latencyAwareResetInterval;
        return this;
    }

    public int getLatencyAwareWindowSize() {
        return _latencyAwareWindowSize;
    }

    public CassandraConfiguration setLatencyAwareWindowSize(int latencyAwareWindowSize) {
        _latencyAwareWindowSize = latencyAwareWindowSize;
        return this;
    }

    public float getLatencyAwareBadnessThreshold() {
        return _latencyAwareBadnessThreshold;
    }

    public CassandraConfiguration setLatencyAwareBadnessThreshold(float latencyAwareBadnessThreshold) {
        _latencyAwareBadnessThreshold = latencyAwareBadnessThreshold;
        return this;
    }

    @Override
    public Optional<Integer> getConnectionLimiterWindowSize() {
        return _connectionLimiterWindowSize;
    }

    public CassandraConfiguration setConnectionLimiterWindowSize(Optional<Integer> connectionLimiterWindowSize) {
        _connectionLimiterWindowSize = connectionLimiterWindowSize;
        return this;
    }

    @Override
    public Optional<Integer> getConnectionLimiterMaxPendingCount() {
        return _connectionLimiterMaxPendingCount;
    }

    public CassandraConfiguration setConnectionLimiterMaxPendingCount(Optional<Integer> connectionLimiterMaxPendingCount) {
        _connectionLimiterMaxPendingCount = connectionLimiterMaxPendingCount;
        return this;
    }

    @Override
    public Optional<Integer> getMaxPendingConnectionsPerHost() {
        return _maxPendingConnectionsPerHost;
    }

    public CassandraConfiguration setMaxPendingConnectionsPerHost(Optional<Integer> maxPendingConnectionsPerHost) {
        _maxPendingConnectionsPerHost = maxPendingConnectionsPerHost;
        return this;
    }

    @Override
    public Optional<Integer> getMaxBlockedThreadsPerHost() {
        return _maxBlockedThreadsPerHost;
    }

    public CassandraConfiguration setMaxBlockedThreadsPerHost(Optional<Integer> maxBlockedThreadsPerHost) {
        _maxBlockedThreadsPerHost = maxBlockedThreadsPerHost;
        return this;
    }

    @Override
    public Optional<Integer> getMaxTimeoutCount() {
        return _maxTimeoutCount;
    }

    public CassandraConfiguration setMaxTimeoutCount(Optional<Integer> maxTimeoutCount) {
        _maxTimeoutCount = maxTimeoutCount;
        return this;
    }

    @Override
    public Optional<Integer> getTimeoutWindow() {
        return _timeoutWindow;
    }

    public CassandraConfiguration setTimeoutWindow(Optional<Integer> timeoutWindow) {
        _timeoutWindow = timeoutWindow;
        return this;
    }

    @Override
    public Optional<Integer> getRetrySuspendWindow() {
        return _retrySuspendWindow;
    }

    public CassandraConfiguration setRetrySuspendWindow(Optional<Integer> retrySuspendWindow) {
        _retrySuspendWindow = retrySuspendWindow;
        return this;
    }

    @Override
    public Optional<Integer> getRetryDelaySlice() {
        return _retryDelaySlice;
    }

    public CassandraConfiguration setRetryDelaySlice(Optional<Integer> retryDelaySlice) {
        _retryDelaySlice = retryDelaySlice;
        return this;
    }

    @Override
    public Optional<Integer> getRetryMaxDelaySlice() {
        return _retryMaxDelaySlice;
    }

    public CassandraConfiguration setRetryMaxDelaySlice(Optional<Integer> retryMaxDelaySlice) {
        _retryMaxDelaySlice = retryMaxDelaySlice;
        return this;
    }

    @Override
    public Optional<Integer> getMaxTimeoutWhenExhausted() {
        return _maxTimeoutWhenExhausted;
    }

    public CassandraConfiguration setMaxTimeoutWhenExhausted(Optional<Integer> maxTimeoutWhenExhausted) {
        _maxTimeoutWhenExhausted = maxTimeoutWhenExhausted;
        return this;
    }

    public SimpleAuthenticationCredentials getAuthenticationCredentials() {
        return _authenticationCredentials;
    }

    public CassandraConfiguration setAuthenticationCredentials(SimpleAuthenticationCredentials authenticationCredentials) {
        _authenticationCredentials = authenticationCredentials;
        return this;
    }

    @Override
    public Optional<Size> getMaxThriftFrameSize() {
        return _maxThriftFrameSize;
    }

    public CassandraConfiguration setMaxThriftFrameSize(Optional<Size> maxThriftFrameSize) {
        _maxThriftFrameSize = maxThriftFrameSize;
        return this;
    }
}
