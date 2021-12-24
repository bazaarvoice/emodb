package com.bazaarvoice.emodb.common.cassandra;

import io.dropwizard.util.Size;

import java.util.Optional;

/**
 * Configuration for a Cassandra keyspace.  The configuration can optionally be used to create a private connection
 * pool by use only by the keyspace.  By default it uses a connection pool shared by all keyspaces in the cluster.
 */
public class KeyspaceConfiguration implements ConnectionPoolConfiguration {

    private String _keyspaceMetric;

    /**
     * Custom connection pool configurations.  Overriding any value results in this keyspace using its own custom
     * connection pool separate from the cluster.  Leaving all values absent results in this keyspace using a
     * connection pool shared with any other keyspaces in the cluster not using a custom pool.
     */
    private Optional<Integer> _initialConnectionsPerHost = Optional.empty();
    private Optional<Integer> _maxConnectionsPerHost = Optional.empty();
    private Optional<Integer> _coreConnectionsPerHost = Optional.empty();
    private Optional<Integer> _socketTimeout = Optional.empty();
    private Optional<Integer> _connectTimeout = Optional.empty();
    private Optional<Integer> _maxFailoverCount = Optional.empty();
    private Optional<Integer> _connectionLimiterWindowSize = Optional.empty();
    private Optional<Integer> _connectionLimiterMaxPendingCount = Optional.empty();
    private Optional<Integer> _maxPendingConnectionsPerHost = Optional.empty();
    private Optional<Integer> _maxBlockedThreadsPerHost = Optional.empty();
    private Optional<Integer> _maxTimeoutCount = Optional.empty();
    private Optional<Integer> _timeoutWindow = Optional.empty();
    private Optional<Integer> _retrySuspendWindow = Optional.empty();
    private Optional<Integer> _retryDelaySlice = Optional.empty();
    private Optional<Integer> _retryMaxDelaySlice = Optional.empty();
    private Optional<Integer> _maxTimeoutWhenExhausted = Optional.empty();
    private Optional<Size> _maxThriftFrameSize = Optional.empty();

    public String getKeyspaceMetric() {
        return _keyspaceMetric;
    }

    public KeyspaceConfiguration setKeyspaceMetric(String keyspaceMetric) {
        _keyspaceMetric = keyspaceMetric;
        return this;
    }

    @Override
    public Optional<Integer> getInitialConnectionsPerHost() {
        return _initialConnectionsPerHost;
    }

    public KeyspaceConfiguration setInitialConnectionsPerHost(Optional<Integer> initialConnectionsPerHost) {
        _initialConnectionsPerHost = initialConnectionsPerHost;
        return this;
    }

    @Override
    public Optional<Integer> getMaxConnectionsPerHost() {
        return _maxConnectionsPerHost;
    }

    public KeyspaceConfiguration setMaxConnectionsPerHost(Optional<Integer> maxConnectionsPerHost) {
        _maxConnectionsPerHost = maxConnectionsPerHost;
        return this;
    }

    @Override
    public Optional<Integer> getCoreConnectionsPerHost() {
        return _coreConnectionsPerHost;
    }

    public KeyspaceConfiguration setCoreConnectionsPerHost(Optional<Integer> coreConnectionsPerHost) {
        _coreConnectionsPerHost = coreConnectionsPerHost;
        return this;
    }

    @Override
    public Optional<Integer> getSocketTimeout() {
        return _socketTimeout;
    }

    public KeyspaceConfiguration setSocketTimeout(Optional<Integer> socketTimeout) {
        _socketTimeout = socketTimeout;
        return this;
    }

    @Override
    public Optional<Integer> getConnectTimeout() {
        return _connectTimeout;
    }

    public KeyspaceConfiguration setConnectTimeout(Optional<Integer> connectTimeout) {
        _connectTimeout = connectTimeout;
        return this;
    }

    @Override
    public Optional<Integer> getMaxFailoverCount() {
        return _maxFailoverCount;
    }

    public KeyspaceConfiguration setMaxFailoverCount(Optional<Integer> maxFailoverCount) {
        _maxFailoverCount = maxFailoverCount;
        return this;
    }

    @Override
    public Optional<Integer> getConnectionLimiterWindowSize() {
        return _connectionLimiterWindowSize;
    }

    public KeyspaceConfiguration setConnectionLimiterWindowSize(Optional<Integer> connectionLimiterWindowSize) {
        _connectionLimiterWindowSize = connectionLimiterWindowSize;
        return this;
    }

    @Override
    public Optional<Integer> getConnectionLimiterMaxPendingCount() {
        return _connectionLimiterMaxPendingCount;
    }

    public KeyspaceConfiguration setConnectionLimiterMaxPendingCount(Optional<Integer> connectionLimiterMaxPendingCount) {
        _connectionLimiterMaxPendingCount = connectionLimiterMaxPendingCount;
        return this;
    }

    @Override
    public Optional<Integer> getMaxPendingConnectionsPerHost() {
        return _maxPendingConnectionsPerHost;
    }

    public KeyspaceConfiguration setMaxPendingConnectionsPerHost(Optional<Integer> maxPendingConnectionsPerHost) {
        _maxPendingConnectionsPerHost = maxPendingConnectionsPerHost;
        return this;
    }

    @Override
    public Optional<Integer> getMaxBlockedThreadsPerHost() {
        return _maxBlockedThreadsPerHost;
    }

    public KeyspaceConfiguration setMaxBlockedThreadsPerHost(Optional<Integer> maxBlockedThreadsPerHost) {
        _maxBlockedThreadsPerHost = maxBlockedThreadsPerHost;
        return this;
    }

    @Override
    public Optional<Integer> getMaxTimeoutCount() {
        return _maxTimeoutCount;
    }

    public KeyspaceConfiguration setMaxTimeoutCount(Optional<Integer> maxTimeoutCount) {
        _maxTimeoutCount = maxTimeoutCount;
        return this;
    }

    @Override
    public Optional<Integer> getTimeoutWindow() {
        return _timeoutWindow;
    }

    public KeyspaceConfiguration setTimeoutWindow(Optional<Integer> timeoutWindow) {
        _timeoutWindow = timeoutWindow;
        return this;
    }

    @Override
    public Optional<Integer> getRetrySuspendWindow() {
        return _retrySuspendWindow;
    }

    public KeyspaceConfiguration setRetrySuspendWindow(Optional<Integer> retrySuspendWindow) {
        _retrySuspendWindow = retrySuspendWindow;
        return this;
    }

    @Override
    public Optional<Integer> getRetryDelaySlice() {
        return _retryDelaySlice;
    }

    public KeyspaceConfiguration setRetryDelaySlice(Optional<Integer> retryDelaySlice) {
        _retryDelaySlice = retryDelaySlice;
        return this;
    }

    @Override
    public Optional<Integer> getRetryMaxDelaySlice() {
        return _retryMaxDelaySlice;
    }

    public KeyspaceConfiguration setRetryMaxDelaySlice(Optional<Integer> retryMaxDelaySlice) {
        _retryMaxDelaySlice = retryMaxDelaySlice;
        return this;
    }

    @Override
    public Optional<Integer> getMaxTimeoutWhenExhausted() {
        return _maxTimeoutWhenExhausted;
    }

    public KeyspaceConfiguration setMaxTimeoutWhenExhausted(Optional<Integer> maxTimeoutWhenExhausted) {
        _maxTimeoutWhenExhausted = maxTimeoutWhenExhausted;
        return this;
    }

    @Override
    public Optional<Size> getMaxThriftFrameSize() {
        return _maxThriftFrameSize;
    }

    public KeyspaceConfiguration setMaxThriftFrameSize(Optional<Size> maxThriftFrameSize) {
        _maxThriftFrameSize = maxThriftFrameSize;
        return this;
    }

    public boolean useSharedConnectionPool() {
        return !(getInitialConnectionsPerHost().isPresent() ||
                getMaxConnectionsPerHost().isPresent() ||
                getSocketTimeout().isPresent() ||
                getConnectTimeout().isPresent() ||
                getMaxFailoverCount().isPresent() ||
                getConnectionLimiterWindowSize().isPresent() ||
                getConnectionLimiterMaxPendingCount().isPresent() ||
                getMaxPendingConnectionsPerHost().isPresent() ||
                getMaxBlockedThreadsPerHost().isPresent() ||
                getMaxTimeoutCount().isPresent() ||
                getTimeoutWindow().isPresent() ||
                getRetrySuspendWindow().isPresent() ||
                getRetryDelaySlice().isPresent() ||
                getRetryMaxDelaySlice().isPresent() ||
                getMaxTimeoutWhenExhausted().isPresent() ||
                getMaxThriftFrameSize().isPresent());
    }
}
