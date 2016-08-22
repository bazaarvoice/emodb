package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.table.db.Placements;
import com.bazaarvoice.emodb.table.db.astyanax.AbstractPlacementFactory;
import com.bazaarvoice.emodb.table.db.astyanax.KeyspaceMap;
import com.bazaarvoice.emodb.table.db.astyanax.Placement;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementUtil;
import com.bazaarvoice.emodb.table.db.astyanax.ValidTablePlacements;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import io.dropwizard.lifecycle.Managed;
import org.apache.cassandra.dht.ByteOrderedPartitioner;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static java.lang.String.format;

/**
 * Creates System-of-Record {@link DeltaPlacement} objects from a Cassandra keyspace and Emo placement string.
 */
public class DeltaPlacementFactory extends AbstractPlacementFactory implements Placements, Managed {
    private final Set<String> _validPlacements;
    private final Map<String, CassandraKeyspace> _keyspaceMap;
    private final DataCenters _dataCenters;

    @Inject
    public DeltaPlacementFactory(LifeCycleRegistry lifeCycle, @ValidTablePlacements Set<String> validPlacements,
                                 @KeyspaceMap Map<String, CassandraKeyspace> keyspaceMap,
                                 DataCenters dataCenters) {
        _validPlacements = validPlacements;
        _keyspaceMap = keyspaceMap;
        _dataCenters = dataCenters;
        lifeCycle.manage(this);
    }

    @Override
    public void start() throws Exception {
        // Range queries depend on using the ByteOrderedPartitioner.  Hard fail if something else is configured.
        for (CassandraKeyspace keyspace : _keyspaceMap.values()) {
            keyspace.errorIfPartitionerMismatch(ByteOrderedPartitioner.class);
        }
    }

    @Override
    public void stop() throws Exception {
        // Nothing to do
    }

    @Override
    public Collection<String> getValidPlacements() {
        return _validPlacements;
    }

    @Override
    public boolean isValidPlacement(String placement) {
        PlacementUtil.parsePlacement(placement);  // make sure it's formatted correctly
        return _validPlacements.contains(placement);
    }

    @Override
    public boolean isAvailablePlacement(String placement) {
        String keyspace = PlacementUtil.parsePlacement(placement)[0];
        return _keyspaceMap.containsKey(keyspace);
    }

    @Override
    public Placement newPlacement(String placement) throws ConnectionException {
        String[] parsed = PlacementUtil.parsePlacement(placement);
        String keyspaceName = parsed[0];
        String cfPrefix = parsed[1];

        CassandraKeyspace keyspace = _keyspaceMap.get(keyspaceName);
        if (keyspace == null) {
            throw new UnknownPlacementException(format(
                    "Placement string refers to unknown or non-local Cassandra keyspace: %s", keyspaceName), placement);
        }

        KeyspaceDefinition keyspaceDef = keyspace.getAstyanaxKeyspace().describeKeyspace();
        ColumnFamily<ByteBuffer, UUID> deltaCf = getColumnFamily(keyspaceDef, cfPrefix, "delta", placement, TimeUUIDSerializer.get());
        ColumnFamily<ByteBuffer, UUID> auditCf = getColumnFamily(keyspaceDef, cfPrefix, "audit", placement, TimeUUIDSerializer.get());
        ColumnFamily<ByteBuffer, UUID> deltaHistoryCf = getColumnFamily(keyspaceDef, cfPrefix, "history", placement, TimeUUIDSerializer.get());

        // Calculate the data centers on demand since they may change in a live system.
        return new DeltaPlacement(placement, keyspace, deltaCf, auditCf, deltaHistoryCf);
    }

    @Override
    public Collection<DataCenter> getDataCenters(String placement) {
        String keyspace = PlacementUtil.parsePlacement(placement)[0];
        return _dataCenters.getForKeyspace(keyspace);
    }

    @Override
    public Optional<String> getLocalCluster(String placement) {
        String keyspaceName = PlacementUtil.parsePlacement(placement)[0];
        CassandraKeyspace keyspace = _keyspaceMap.get(keyspaceName);
        return keyspace != null ? Optional.of(keyspace.getClusterName()) : Optional.<String>absent();
    }
}
