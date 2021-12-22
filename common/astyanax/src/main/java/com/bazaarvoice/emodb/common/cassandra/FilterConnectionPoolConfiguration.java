package com.bazaarvoice.emodb.common.cassandra;

import io.dropwizard.util.Size;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Implementation of ConnectionPoolConfiguration that defers to another instance with the ability to override
 * specific settings.
 */
public class FilterConnectionPoolConfiguration implements ConnectionPoolConfiguration {

    private final ConnectionPoolConfiguration _config;

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

    public FilterConnectionPoolConfiguration(ConnectionPoolConfiguration config) {
        _config = config;
    }

    @Override
    public Optional<Integer> getInitialConnectionsPerHost() {
        return or(_initialConnectionsPerHost, _config.getInitialConnectionsPerHost());
    }

    public void setInitialConnectionsPerHost(int initialConnectionsPerHost) {
        _initialConnectionsPerHost = Optional.of(initialConnectionsPerHost);
    }

    @Override
    public Optional<Integer> getMaxConnectionsPerHost() {
        return or(_maxConnectionsPerHost, _config.getMaxConnectionsPerHost());
    }

    public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
        _maxConnectionsPerHost = Optional.of(maxConnectionsPerHost);
    }

    @Override
    public Optional<Integer> getCoreConnectionsPerHost() {
        return or(_coreConnectionsPerHost, _config.getCoreConnectionsPerHost());
    }

    public void setCoreConnectionsPerHost(int coreConnectionsPerHost) {
        _coreConnectionsPerHost = Optional.of(coreConnectionsPerHost);
    }

    @Override
    public Optional<Integer> getSocketTimeout() {
        return or(_socketTimeout, _config.getSocketTimeout());
    }

    public void setSocketTimeout(int socketTimeout) {
        _socketTimeout = Optional.of(socketTimeout);
    }

    @Override
    public Optional<Integer> getConnectTimeout() {
        return or(_connectTimeout, _config.getConnectTimeout());
    }

    public void setConnectTimeout(int connectTimeout) {
        _connectTimeout = Optional.of(connectTimeout);
    }

    @Override
    public Optional<Integer> getMaxFailoverCount() {
        return or(_maxFailoverCount, _config.getMaxFailoverCount());
    }

    public void setMaxFailoverCount(int maxFailoverCount) {
        _maxFailoverCount = Optional.of(maxFailoverCount);
    }

    @Override
    public Optional<Integer> getConnectionLimiterWindowSize() {
        return or(_connectionLimiterWindowSize, _config.getConnectionLimiterWindowSize());
    }

    public void setConnectionLimiterWindowSize(int connectionLimiterWindowSize) {
        _connectionLimiterWindowSize = Optional.of(connectionLimiterWindowSize);
    }

    @Override
    public Optional<Integer> getConnectionLimiterMaxPendingCount() {
        return or(_connectionLimiterMaxPendingCount, _config.getConnectionLimiterMaxPendingCount());
    }

    public void setConnectionLimiterMaxPendingCount(int connectionLimiterMaxPendingCount) {
        _connectionLimiterMaxPendingCount = Optional.of(connectionLimiterMaxPendingCount);
    }

    @Override
    public Optional<Integer> getMaxPendingConnectionsPerHost() {
        return or(_maxPendingConnectionsPerHost, _config.getMaxPendingConnectionsPerHost());
    }

    public void setMaxPendingConnectionsPerHost(int maxPendingConnectionsPerHost) {
        _maxPendingConnectionsPerHost = Optional.of(maxPendingConnectionsPerHost);
    }

    @Override
    public Optional<Integer> getMaxBlockedThreadsPerHost() {
        return or(_maxBlockedThreadsPerHost, _config.getMaxBlockedThreadsPerHost());
    }

    public void setMaxBlockedThreadsPerHost(int maxBlockedThreadsPerHost) {
        _maxBlockedThreadsPerHost = Optional.of(maxBlockedThreadsPerHost);
    }

    @Override
    public Optional<Integer> getMaxTimeoutCount() {
        return or(_maxTimeoutCount, _config.getMaxTimeoutCount());
    }

    public void setMaxTimeoutCount(int maxTimeoutCount) {
        _maxTimeoutCount = Optional.of(maxTimeoutCount);
    }

    @Override
    public Optional<Integer> getTimeoutWindow() {
        return or(_timeoutWindow, _config.getTimeoutWindow());
    }

    public void setTimeoutWindow(int timeoutWindow) {
        _timeoutWindow = Optional.of(timeoutWindow);
    }

    @Override
    public Optional<Integer> getRetrySuspendWindow() {
        return or(_retrySuspendWindow, _config.getRetrySuspendWindow());
    }

    public void setRetrySuspendWindow(int retrySuspendWindow) {
        _retrySuspendWindow = Optional.of(retrySuspendWindow);
    }

    @Override
    public Optional<Integer> getRetryDelaySlice() {
        return or(_retryDelaySlice, _config.getRetryDelaySlice());
    }

    public void setRetryDelaySlice(int retryDelaySlice) {
        _retryDelaySlice = Optional.of(retryDelaySlice);
    }

    @Override
    public Optional<Integer> getRetryMaxDelaySlice() {
        return or(_retryMaxDelaySlice, _config.getRetryMaxDelaySlice());
    }

    public void setRetryMaxDelaySlice(int retryMaxDelaySlice) {
        _retryMaxDelaySlice = Optional.of(retryMaxDelaySlice);
    }

    @Override
    public Optional<Integer> getMaxTimeoutWhenExhausted() {
        return or(_maxTimeoutWhenExhausted, _config.getMaxTimeoutWhenExhausted());
    }

    public void setMaxTimeoutWhenExhausted(int maxTimeoutWhenExhausted) {
        _maxTimeoutWhenExhausted = Optional.of(maxTimeoutWhenExhausted);
    }

    @Override
    public Optional<Size> getMaxThriftFrameSize() {
        return or(_maxThriftFrameSize, _config.getMaxThriftFrameSize());
    }

    public void setMaxThriftFrameSize(Size maxThriftFrameSize) {
        _maxThriftFrameSize = Optional.of(maxThriftFrameSize);
    }

    private static <T> Optional<T> or(Optional<T>... values) {
        return Stream.of(values)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }
}
