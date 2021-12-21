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
        return Stream.of(_initialConnectionsPerHost, _config.getInitialConnectionsPerHost())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setInitialConnectionsPerHost(int initialConnectionsPerHost) {
        _initialConnectionsPerHost = Optional.of(initialConnectionsPerHost);
    }

    @Override
    public Optional<Integer> getMaxConnectionsPerHost() {
        return Stream.of(_maxConnectionsPerHost, _config.getMaxConnectionsPerHost())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setMaxConnectionsPerHost(int maxConnectionsPerHost) {
        _maxConnectionsPerHost = Optional.of(maxConnectionsPerHost);
    }

    @Override
    public Optional<Integer> getCoreConnectionsPerHost() {
        return Stream.of(_coreConnectionsPerHost, _config.getCoreConnectionsPerHost())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setCoreConnectionsPerHost(int coreConnectionsPerHost) {
        _coreConnectionsPerHost = Optional.of(coreConnectionsPerHost);
    }

    @Override
    public Optional<Integer> getSocketTimeout() {
        return Stream.of(_socketTimeout, _config.getSocketTimeout())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setSocketTimeout(int socketTimeout) {
        _socketTimeout = Optional.of(socketTimeout);
    }

    @Override
    public Optional<Integer> getConnectTimeout() {
        return Stream.of(_connectTimeout, _config.getConnectTimeout())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setConnectTimeout(int connectTimeout) {
        _connectTimeout = Optional.of(connectTimeout);
    }

    @Override
    public Optional<Integer> getMaxFailoverCount() {
        return Stream.of(_maxFailoverCount, _config.getMaxFailoverCount())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setMaxFailoverCount(int maxFailoverCount) {
        _maxFailoverCount = Optional.of(maxFailoverCount);
    }

    @Override
    public Optional<Integer> getConnectionLimiterWindowSize() {
        return Stream.of(_connectionLimiterWindowSize, _config.getConnectionLimiterWindowSize())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setConnectionLimiterWindowSize(int connectionLimiterWindowSize) {
        _connectionLimiterWindowSize = Optional.of(connectionLimiterWindowSize);
    }

    @Override
    public Optional<Integer> getConnectionLimiterMaxPendingCount() {
        return Stream.of(_connectionLimiterMaxPendingCount, _config.getConnectionLimiterMaxPendingCount())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setConnectionLimiterMaxPendingCount(int connectionLimiterMaxPendingCount) {
        _connectionLimiterMaxPendingCount = Optional.of(connectionLimiterMaxPendingCount);
    }

    @Override
    public Optional<Integer> getMaxPendingConnectionsPerHost() {
        return Stream.of(_maxPendingConnectionsPerHost, _config.getMaxPendingConnectionsPerHost())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setMaxPendingConnectionsPerHost(int maxPendingConnectionsPerHost) {
        _maxPendingConnectionsPerHost = Optional.of(maxPendingConnectionsPerHost);
    }

    @Override
    public Optional<Integer> getMaxBlockedThreadsPerHost() {
        return Stream.of(_maxBlockedThreadsPerHost, _config.getMaxBlockedThreadsPerHost())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setMaxBlockedThreadsPerHost(int maxBlockedThreadsPerHost) {
        _maxBlockedThreadsPerHost = Optional.of(maxBlockedThreadsPerHost);
    }

    @Override
    public Optional<Integer> getMaxTimeoutCount() {
        return Stream.of(_maxTimeoutCount, _config.getMaxTimeoutCount())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setMaxTimeoutCount(int maxTimeoutCount) {
        _maxTimeoutCount = Optional.of(maxTimeoutCount);
    }

    @Override
    public Optional<Integer> getTimeoutWindow() {
        return Stream.of(_timeoutWindow, _config.getTimeoutWindow())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setTimeoutWindow(int timeoutWindow) {
        _timeoutWindow = Optional.of(timeoutWindow);
    }

    @Override
    public Optional<Integer> getRetrySuspendWindow() {
        return Stream.of(_retrySuspendWindow, _config.getRetrySuspendWindow())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setRetrySuspendWindow(int retrySuspendWindow) {
        _retrySuspendWindow = Optional.of(retrySuspendWindow);
    }

    @Override
    public Optional<Integer> getRetryDelaySlice() {
        return Stream.of(_retryDelaySlice, _config.getRetryDelaySlice())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setRetryDelaySlice(int retryDelaySlice) {
        _retryDelaySlice = Optional.of(retryDelaySlice);
    }

    @Override
    public Optional<Integer> getRetryMaxDelaySlice() {
        return Stream.of(_retryMaxDelaySlice, _config.getRetryMaxDelaySlice())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setRetryMaxDelaySlice(int retryMaxDelaySlice) {
        _retryMaxDelaySlice = Optional.of(retryMaxDelaySlice);
    }

    @Override
    public Optional<Integer> getMaxTimeoutWhenExhausted() {
        return Stream.of(_maxTimeoutWhenExhausted, _config.getMaxTimeoutWhenExhausted())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setMaxTimeoutWhenExhausted(int maxTimeoutWhenExhausted) {
        _maxTimeoutWhenExhausted = Optional.of(maxTimeoutWhenExhausted);
    }

    @Override
    public Optional<Size> getMaxThriftFrameSize() {
        return Stream.of(_maxThriftFrameSize, _config.getMaxThriftFrameSize())
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();
    }

    public void setMaxThriftFrameSize(Size maxThriftFrameSize) {
        _maxThriftFrameSize = Optional.of(maxThriftFrameSize);
    }
}
