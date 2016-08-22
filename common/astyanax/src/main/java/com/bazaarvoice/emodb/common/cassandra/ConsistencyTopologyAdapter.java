package com.bazaarvoice.emodb.common.cassandra;

import com.netflix.astyanax.model.ConsistencyLevel;

public class ConsistencyTopologyAdapter {

    private final boolean _networkTopology;
    private final int _replicationFactor;

    public ConsistencyTopologyAdapter(CassandraReplication replication) {
        _networkTopology = replication.isNetworkTopology();
        _replicationFactor = replication.getReplicationFactor();
    }

    /**
     * Reduce the desired consistency level to be compatible with the deployed ring topology.
     */
    public ConsistencyLevel clamp(ConsistencyLevel consistencyLevel) {
        // Cassandra only allows the use of LOCAL_QUORUM and EACH_QUORUM if the keyspace
        // placement strategy is NetworkTopologyStrategy
        if ((consistencyLevel == ConsistencyLevel.CL_LOCAL_QUORUM || consistencyLevel == ConsistencyLevel.CL_EACH_QUORUM) && !_networkTopology) {
            consistencyLevel = ConsistencyLevel.CL_QUORUM;
        }

        // we may want to write to at two or three servers to ensure the write survives the
        // permanent failure of any single server.  but if the ring has fewer servers to
        // begin with (ie. it's a test ring) we must reduce the consistency level.
        if (consistencyLevel == ConsistencyLevel.CL_THREE && _replicationFactor < 3) {
            consistencyLevel = ConsistencyLevel.CL_TWO;
        }
        if (consistencyLevel == ConsistencyLevel.CL_TWO && _replicationFactor < 2) {
            consistencyLevel = ConsistencyLevel.CL_ONE;
        }

        return consistencyLevel;
    }
}
