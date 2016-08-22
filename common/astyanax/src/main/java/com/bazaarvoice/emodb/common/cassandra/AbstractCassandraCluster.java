package com.bazaarvoice.emodb.common.cassandra;

/**
 * Useful base class for {@link CassandraCluster} implementations.  Even though the cluster name and data center can
 * typically be introspected from the cluster to do so requires that a connection is established first, which can
 * be problematic if these values are required as part of system initialization and configuration.  This implementation
 * allows these values to be provided at construction time.
 */
abstract public class AbstractCassandraCluster<C> implements CassandraCluster<C> {

    private final String _clusterName;
    private final String _dataCenter;

    public AbstractCassandraCluster(String clusterName, String dataCenter) {
        _clusterName = clusterName;
        _dataCenter = dataCenter;
    }

    @Override
    public String getClusterName() {
        return _clusterName;
    }

    @Override
    public String getDataCenter() {
        return _dataCenter;
    }
}
