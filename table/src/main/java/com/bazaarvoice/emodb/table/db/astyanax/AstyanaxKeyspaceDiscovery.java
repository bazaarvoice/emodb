package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.datacenter.api.KeyspaceDiscovery;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.KeyspaceDefinition;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Enumerates Cassandra keyspaces, filtered by data center.
 */
public class AstyanaxKeyspaceDiscovery implements KeyspaceDiscovery {
    private final Map<String, CassandraKeyspace> _keyspaceMap;

    @Inject
    public AstyanaxKeyspaceDiscovery(@KeyspaceMap Map<String, CassandraKeyspace> keyspaceMap) {
        _keyspaceMap = requireNonNull(keyspaceMap, "keyspaceMap");
    }

    @Override
    public Collection<String> getKeyspacesForDataCenter(String cassandraDataCenter) {
        Set<String> keyspaces = Sets.newHashSet();
        for (Map.Entry<String, CassandraKeyspace> entry : _keyspaceMap.entrySet()) {
            KeyspaceDefinition keyspaceDefinition = describe(entry.getValue());
            if (replicatesTo(keyspaceDefinition, cassandraDataCenter)) {
                keyspaces.add(entry.getKey());
            }
        }
        return keyspaces;
    }

    private boolean replicatesTo(KeyspaceDefinition keyspaceDefinition, String dataCenter) {
        if (keyspaceDefinition.getStrategyClass().endsWith("NetworkTopologyStrategy")) {
            String numReplicas = keyspaceDefinition.getStrategyOptions().get(dataCenter);
            return numReplicas != null && Integer.valueOf(numReplicas) != 0;
        }
        // Other strategies don't vary replication factor by data center.  Assume true.
        return true;
    }

    private KeyspaceDefinition describe(CassandraKeyspace keyspace) {
        try {
            return keyspace.getAstyanaxKeyspace().describeKeyspace();
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }
    }
}
