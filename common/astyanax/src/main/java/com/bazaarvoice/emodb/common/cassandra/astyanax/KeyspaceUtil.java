package com.bazaarvoice.emodb.common.cassandra.astyanax;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.HostConnectionPool;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NoAvailableHostsException;
import com.netflix.astyanax.shallows.EmptyKeyspaceTracerFactory;
import com.netflix.astyanax.thrift.ThriftKeyspaceImpl;
import org.apache.cassandra.thrift.Cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Helper utility for Keyspace operations
 */
public final class KeyspaceUtil {

    // Static utility
    private KeyspaceUtil() {
        // empty
    }

    /**
     * Starts the process of pinning calls to a Keyspace to a specific host.
     */
    public static PinnedKeyspaceBuilder pin(Keyspace keyspace) {
        return new PinnedKeyspaceBuilder(keyspace);
    }

    public static class PinnedKeyspaceBuilder {
        private final Keyspace _keyspace;

        private PinnedKeyspaceBuilder(Keyspace keyspace) {
            _keyspace = checkNotNull(keyspace, "keyspace");
        }

        public Keyspace toHost(String hostName) throws ConnectionException {
            Host host = _keyspace.getConnectionPool().getPools().stream()
                    .map(HostConnectionPool::getHost)
                    .filter(poolHost -> hostName.equals(poolHost.getHostName()) || hostName.equals(poolHost.getIpAddress()))
                    .findFirst().orElseThrow(() -> new NoAvailableHostsException("No hosts pools found"));

            return pinToVerifiedHost(host);
        }

        /**
         * Returns a view of the provided Keyspace that pins all operations to the provided host.
         */
        public Keyspace toHost(Host host) throws ConnectionException {
            if (!_keyspace.getConnectionPool().getPools().stream()
                            .map(HostConnectionPool::getHost)
                            .anyMatch(poolHost -> poolHost.equals(host))) {
                throw new NoAvailableHostsException("Host not found in pool");
            }

            return pinToVerifiedHost(host);
        }

        private Keyspace pinToVerifiedHost(Host host) throws ConnectionException {
            //noinspection unchecked
            PinnedConnectionPool<Cassandra.Client> pinnedPool = new PinnedConnectionPool(_keyspace.getConnectionPool(), host);
            return new ThriftKeyspaceImpl(
                    _keyspace.getKeyspaceName(), pinnedPool, _keyspace.getConfig(), EmptyKeyspaceTracerFactory.getInstance());
        }
    }
}
