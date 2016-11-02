package com.bazaarvoice.emodb.common.cassandra;

import com.google.common.base.Optional;
import io.dropwizard.util.Size;

/**
 * Implementation of ConnectionPoolConfiguration that defers to another instance with the ability to override
 * specific settings.
 */
public class FilterConnectionPoolConfiguration implements ConnectionPoolConfiguration {

    private final ConnectionPoolConfiguration _config;

    private Optional<Integer> _initialConnectionsPerHost = Optional.absent();
    private Optional<Integer> _maxConnectionsPerHost = Optional.absent();
    private Optional<Integer> _coreConnectionsPerHost = Optional.absent();
    private Optional<Integer> _socketTimeout = Optional.absent();
    private Optional<Integer> _connectTimeout = Optional.absent();
    private Optional<Integer> _maxFailoverCount = Optional.absent();
    private Optional<Integer> _connectionLimiterWindowSize = Optional.absent();
    private Optional<Integer> _connectionLimiterMaxPendingCount = Optional.absent();
    private Optional<Integer> _maxPendingConnectionsPerHost = Optional.absent();
    private Optional<Integer> _maxBlockedThreadsPerHost = Optional.absent();
    private Optional<Integer> _maxTimeoutCount = Optional.absent();
    private Optional<Integer> _timeoutWindow = Optional.absent();
    private Optional<Integer> _retrySuspendWindow = Optional.absent();
    private Optional<Integer> _retryDelaySlice = Optional.absent();
    private Optional<Integer> _retryMaxDelaySlice = Optional.absent();
    private Optional<Integer> _maxTimeoutWhenExhausted = Optional.absent();
    private Optional<Size> _maxThriftFrameSize = Optional.absent();

    public FilterConnectionPoolConfiguration(ConnectionPoolConfiguration config) {
        _config = config;
    }

    @Override
    public Optional<Integer> getInitialConnectionsPerHost() {
        return _initialConnectionsPerHost.or(_config.getInitialConnectionsPerHost());
    }

    public void setInitialConnectionsPerHost(int initialConnectionsPerHost) {
        _initialConnectionsPerHost = Optional.of(initialConnectionsPerHost);
    }

    @Override
    public Optional<Integer> getMaxConnectionsPerHost() {
        return _maxConnectionsPerHost.or(_config.getMaxConnectionsPerHost());
    }

    public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
        _maxConnectionsPerHost = Optional.of(maxConnectionsPerHost);
    }

    @Override
    public Optional<Integer> getCoreConnectionsPerHost() {
        return _coreConnectionsPerHost.or(_config.getCoreConnectionsPerHost());
    }

    public void setCoreConnectionsPerHost(int coreConnectionsPerHost) {
        _coreConnectionsPerHost = Optional.of(coreConnectionsPerHost);
    }

    @Override
    public Optional<Integer> getSocketTimeout() {
        return _socketTimeout.or(_config.getSocketTimeout());
    }

    public void setSocketTimeout(int socketTimeout) {
        _socketTimeout = Optional.of(socketTimeout);
    }

    @Override
    public Optional<Integer> getConnectTimeout() {
        return _connectTimeout.or(_config.getConnectTimeout());
    }

    public void setConnectTimeout(int connectTimeout) {
        _connectTimeout = Optional.of(connectTimeout);
    }

    @Override
    public Optional<Integer> getMaxFailoverCount() {
        return _maxFailoverCount.or(_config.getMaxFailoverCount());
    }

    public void setMaxFailoverCount(int maxFailoverCount) {
        _maxFailoverCount = Optional.of(maxFailoverCount);
    }

    @Override
    public Optional<Integer> getConnectionLimiterWindowSize() {
        return _connectionLimiterWindowSize.or(_config.getConnectionLimiterWindowSize());
    }

    public void setConnectionLimiterWindowSize(int connectionLimiterWindowSize) {
        _connectionLimiterWindowSize = Optional.of(connectionLimiterWindowSize);
    }

    @Override
    public Optional<Integer> getConnectionLimiterMaxPendingCount() {
        return _connectionLimiterMaxPendingCount.or(_config.getConnectionLimiterMaxPendingCount());
    }

    public void setConnectionLimiterMaxPendingCount(int connectionLimiterMaxPendingCount) {
        _connectionLimiterMaxPendingCount = Optional.of(connectionLimiterMaxPendingCount);
    }

    @Override
    public Optional<Integer> getMaxPendingConnectionsPerHost() {
        return _maxPendingConnectionsPerHost.or(_config.getMaxPendingConnectionsPerHost());
    }

    public void setMaxPendingConnectionsPerHost(int maxPendingConnectionsPerHost) {
        _maxPendingConnectionsPerHost = Optional.of(maxPendingConnectionsPerHost);
    }

    @Override
    public Optional<Integer> getMaxBlockedThreadsPerHost() {
        return _maxBlockedThreadsPerHost.or(_config.getMaxBlockedThreadsPerHost());
    }

    public void setMaxBlockedThreadsPerHost(int maxBlockedThreadsPerHost) {
        _maxBlockedThreadsPerHost = Optional.of(maxBlockedThreadsPerHost);
    }

    @Override
    public Optional<Integer> getMaxTimeoutCount() {
        return _maxTimeoutCount.or(_config.getMaxTimeoutCount());
    }

    public void setMaxTimeoutCount(int maxTimeoutCount) {
        _maxTimeoutCount = Optional.of(maxTimeoutCount);
    }

    @Override
    public Optional<Integer> getTimeoutWindow() {
        return _timeoutWindow.or(_config.getTimeoutWindow());
    }

    public void setTimeoutWindow(int timeoutWindow) {
        _timeoutWindow = Optional.of(timeoutWindow);
    }

    @Override
    public Optional<Integer> getRetrySuspendWindow() {
        return _retrySuspendWindow.or(_config.getRetrySuspendWindow());
    }

    public void setRetrySuspendWindow(int retrySuspendWindow) {
        _retrySuspendWindow = Optional.of(retrySuspendWindow);
    }

    @Override
    public Optional<Integer> getRetryDelaySlice() {
        return _retryDelaySlice.or(_config.getRetryDelaySlice());
    }

    public void setRetryDelaySlice(int retryDelaySlice) {
        _retryDelaySlice = Optional.of(retryDelaySlice);
    }

    @Override
    public Optional<Integer> getRetryMaxDelaySlice() {
        return _retryMaxDelaySlice.or(_config.getRetryMaxDelaySlice());
    }

    public void setRetryMaxDelaySlice(int retryMaxDelaySlice) {
        _retryMaxDelaySlice = Optional.of(retryMaxDelaySlice);
    }

    @Override
    public Optional<Integer> getMaxTimeoutWhenExhausted() {
        return _maxTimeoutWhenExhausted.or(_config.getMaxTimeoutWhenExhausted());
    }

    public void setMaxTimeoutWhenExhausted(int maxTimeoutWhenExhausted) {
        _maxTimeoutWhenExhausted = Optional.of(maxTimeoutWhenExhausted);
    }

    @Override
    public Optional<Size> getMaxThriftFrameSize() {
        return _maxThriftFrameSize.or(_config.getMaxThriftFrameSize());
    }

    public void setMaxThriftFrameSize(Size maxThriftFrameSize) {
        _maxThriftFrameSize = Optional.of(maxThriftFrameSize);
    }
}
