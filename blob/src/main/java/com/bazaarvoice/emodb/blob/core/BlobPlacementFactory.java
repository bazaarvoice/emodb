package com.bazaarvoice.emodb.blob.core;

import com.amazonaws.services.s3.AmazonS3;
import com.bazaarvoice.emodb.blob.db.s3.PlacementsToS3BucketNames;
import com.bazaarvoice.emodb.blob.db.s3.S3BucketNamesToS3Clients;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.datacenter.api.DataCenter;
import com.bazaarvoice.emodb.datacenter.api.DataCenters;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.table.db.astyanax.AbstractPlacementFactory;
import com.bazaarvoice.emodb.table.db.astyanax.KeyspaceMap;
import com.bazaarvoice.emodb.table.db.astyanax.PlacementUtil;
import com.bazaarvoice.emodb.table.db.astyanax.ValidTablePlacements;
import com.google.inject.Inject;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Composite;
import com.netflix.astyanax.serializers.SpecificCompositeSerializer;
import io.dropwizard.lifecycle.Managed;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.dht.ByteOrderedPartitioner;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.lang.String.format;

/**
 * Creates Blob Store {@link BlobPlacement} objects from a Cassandra keyspace and Emo placement string.
 */
public class BlobPlacementFactory extends AbstractPlacementFactory implements Managed {
    private final Set<String> _validPlacements;
    private final Map<String, CassandraKeyspace> _keyspaceMap;
    private final DataCenters _dataCenters;
    private final Map<String, String> _placementsToBucketNames;
    private final Map<String, AmazonS3> _bucketNamesToS3Clients;

    @Inject
    public BlobPlacementFactory(LifeCycleRegistry lifeCycle,
                                @ValidTablePlacements Set<String> validPlacements,
                                @KeyspaceMap Map<String, CassandraKeyspace> keyspaceMap,
                                DataCenters dataCenters,
                                @PlacementsToS3BucketNames final Map<String, String> placementsToBuckets,
                                @S3BucketNamesToS3Clients final Map<String, AmazonS3> bucketNamesToS3Clients) {
        _validPlacements = validPlacements;
        _keyspaceMap = keyspaceMap;
        _dataCenters = dataCenters;
        _placementsToBucketNames = Objects.requireNonNull(placementsToBuckets);
        _bucketNamesToS3Clients = Objects.requireNonNull(bucketNamesToS3Clients);
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
    public BlobPlacement newPlacement(String placement) throws ConnectionException {
        String[] parsed = PlacementUtil.parsePlacement(placement);
        String keyspaceName = parsed[0];
        String cfPrefix = parsed[1];

        CassandraKeyspace keyspace = _keyspaceMap.get(keyspaceName);
        if (keyspace == null) {
            throw new UnknownPlacementException(format(
                    "Placement string refers to unknown or non-local Cassandra keyspace: %s", keyspaceName), placement);
        }

        KeyspaceDefinition keyspaceDef = keyspace.getAstyanaxKeyspace().describeKeyspace();
        ColumnFamily<ByteBuffer, Composite> columnFamily = getColumnFamily(keyspaceDef, cfPrefix, "blob", placement,
                new SpecificCompositeSerializer(CompositeType.getInstance(Arrays.<AbstractType<?>>asList(
                        AsciiType.instance, IntegerType.instance))));

        return new BlobPlacement(placement, keyspace, columnFamily, getS3Bucket(placement), getS3Client(placement));
    }

    @Override
    public Collection<DataCenter> getDataCenters(String placement) {
        String keyspace = PlacementUtil.parsePlacement(placement)[0];
        return _dataCenters.getForKeyspace(keyspace);
    }

    private AmazonS3 getS3Client(final String placement) {
        return _bucketNamesToS3Clients.get(getS3Bucket(placement));
    }

    private String getS3Bucket(final String placement) {
        return _placementsToBucketNames.get(placement);
    }
}
