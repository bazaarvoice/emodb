package com.bazaarvoice.emodb.common.cassandra;

import com.netflix.astyanax.ddl.KeyspaceDefinition;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

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
            Map<String, Integer> dataCenterMap = new HashMap<>();
            for (Map.Entry<String, String> option : keyspaceDefinition.getStrategyOptions().entrySet()) {
                String dataCenter = option.getKey();
                int repFactor = Integer.parseInt(option.getValue());
                replicationFactor += repFactor;
                dataCenterMap.put(dataCenter, repFactor);
            }
            _replicationFactor = replicationFactor;
            _replicationFactorByDataCenter = Collections.unmodifiableMap(dataCenterMap);
        } else {
            // SimpleStrategy and OldNetworkTopologyStrategy both require a 'replication_factor' setting
            _replicationFactor = Integer.parseInt(keyspaceDefinition.getStrategyOptions().get("replication_factor"));
            _replicationFactorByDataCenter = Collections.EMPTY_MAP;
        }
    }

    public boolean isNetworkTopology() {
        return _networkTopology;
    }

    public int getReplicationFactor() {
        return _replicationFactor;
    }

    public int getReplicationFactorForDataCenter(String dataCenter) {
        return Optional.ofNullable(_replicationFactorByDataCenter.get(dataCenter)).orElse(0);
    }
}
