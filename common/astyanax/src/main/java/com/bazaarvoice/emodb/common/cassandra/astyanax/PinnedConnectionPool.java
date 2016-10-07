package com.bazaarvoice.emodb.common.cassandra.astyanax;

import com.netflix.astyanax.connectionpool.ConnectionPool;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.Operation;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.impl.AbstractOperationFilter;
import com.netflix.astyanax.connectionpool.impl.Topology;
import com.netflix.astyanax.partitioner.Partitioner;
import com.netflix.astyanax.retry.RetryPolicy;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * ConnectionPool implementation which delegates all operations to a specific host.
 * @param <T>
 */
public class PinnedConnectionPool<T> implements ConnectionPool<T> {

    private final ConnectionPool<T> _delegate;
    private final Host _host;

    public PinnedConnectionPool(ConnectionPool<T> delegate, Host host) {
        checkArgument(delegate.hasHost(host), "Cannot pin to host not in pool");
        _delegate = delegate;
        _host = host;
    }

    @Override
    public <R> OperationResult<R> executeWithFailover(Operation<T, R> operation, RetryPolicy retryPolicy)
            throws ConnectionException, OperationException {
        Operation<T, R> pinnedOperation = new AbstractOperationFilter<T, R>(operation) {
            @Override
            public Host getPinnedHost() {
                return _host;
            }
        };

        return _delegate.executeWithFailover(pinnedOperation, retryPolicy);
    }

    // All remaining operations delegate to the original pool.

    @Override
    public boolean addHost(Host host, boolean b) {
        return _delegate.addHost(host, b);
    }

    @Override
    public boolean removeHost(Host host, boolean b) {
        return _delegate.removeHost(host, b);
    }

    @Override
    public boolean isHostUp(Host host) {
        return _delegate.isHostUp(host);
    }

    @Override
    public boolean hasHost(Host host) {
        return _delegate.hasHost(host);
    }

    @Override
    public List<HostConnectionPool<T>> getActivePools() {
        return _delegate.getActivePools();
    }

    @Override
    public List<HostConnectionPool<T>> getPools() {
        return _delegate.getPools();
    }

    @Override
    public void setHosts(Collection<Host> collection) {
        _delegate.setHosts(collection);
    }

    @Override
    public HostConnectionPool<T> getHostPool(Host host) {
        return _delegate.getHostPool(host);
    }

    @Override
    public void shutdown() {
        _delegate.shutdown();
    }

    @Override
    public void start() {
        _delegate.start();
    }

    @Override
    public Topology<T> getTopology() {
        return _delegate.getTopology();
    }

    @Override
    public Partitioner getPartitioner() {
        return _delegate.getPartitioner();
    }
}
