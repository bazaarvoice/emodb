package com.bazaarvoice.emodb.common.cassandra;

import com.google.common.base.Throwables;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import io.dropwizard.lifecycle.Managed;

/**
 * {@link CassandraCluster} implementation for connecting to Cassandra using Astyanax.
 */
public class AstyanaxCluster extends AbstractCassandraCluster<Keyspace> implements Managed {

    private final AstyanaxContext<Cluster> _astyanaxContext;

    public AstyanaxCluster(AstyanaxContext<Cluster> astyanaxContext, String clusterName, String dataCenter) {
        super(clusterName, dataCenter);
        _astyanaxContext = astyanaxContext;
    }

    @Override
    public void start() throws Exception {
        _astyanaxContext.start();
    }

    @Override
    public void stop() throws Exception {
        _astyanaxContext.shutdown();
    }

    @Override
    public Keyspace connect(String keyspaceName) {
        try {
            return _astyanaxContext.getClient().getKeyspace(keyspaceName);
        } catch (ConnectionException e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
