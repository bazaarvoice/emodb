package com.bazaarvoice.emodb.common.cassandra;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.ddl.KeyspaceDefinition;

import java.util.Map;

/**
 * Provides information about the replication strategy for a column family.
 */
public class CassandraReplication {
    private final boolean _networkTopology;
    private final int _replicationFactor;
    private final Map<String, Integer> _replicationFactorByDataCenter;

    public CassandraReplication(KeyspaceDefinition keyspaceDefinition) {
        _networkTopology = keyspaceDefinition.getStrategyClass().endsWith("NetworkTopologyStrategy");

        if (_networkTopology) {
            // This algorithm should match the NetworkTopologyStrategy.getReplicationFactor() method.
            // Strategy options is a Map of data center name -> replication factor.
            int replicationFactor = 0;
            ImmutableMap.Builder<String, Integer> dataCenterBuilder = ImmutableMap.builder();
            for (Map.Entry<String, String> option : keyspaceDefinition.getStrategyOptions().entrySet()) {
                String dataCenter = option.getKey();
                int repFactor = Integer.parseInt(option.getValue());
                replicationFactor += repFactor;
                dataCenterBuilder.put(dataCenter, repFactor);
            }
            _replicationFactor = replicationFactor;
            _replicationFactorByDataCenter = dataCenterBuilder.build();
        } else {
            // SimpleStrategy and OldNetworkTopologyStrategy both require a 'replication_factor' setting
            _replicationFactor = Integer.parseInt(keyspaceDefinition.getStrategyOptions().get("replication_factor"));
            _replicationFactorByDataCenter = ImmutableMap.of();
        }
    }

    public boolean isNetworkTopology() {
        return _networkTopology;
    }

    public int getReplicationFactor() {
        return _replicationFactor;
    }

    public int getReplicationFactorForDataCenter(String dataCenter) {
        return Objects.firstNonNull(_replicationFactorByDataCenter.get(dataCenter), 0);
    }
}
