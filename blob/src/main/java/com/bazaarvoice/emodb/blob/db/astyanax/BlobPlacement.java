package com.bazaarvoice.emodb.blob.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.table.db.astyanax.Placement;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Composite;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A Cassandra keyspace and the BlobStore column family.
 */
class BlobPlacement implements Placement {
    private final String _name;
    private final CassandraKeyspace _keyspace;
    private final ColumnFamily<ByteBuffer, Composite> _blobColumnFamily;

    BlobPlacement(String name,
                  CassandraKeyspace keyspace,
                  ColumnFamily<ByteBuffer, Composite> blobColumnFamily) {
        _name = checkNotNull(name, "name");
        _keyspace = checkNotNull(keyspace, "keyspace");
        _blobColumnFamily = checkNotNull(blobColumnFamily, "blobColumnFamily");
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public CassandraKeyspace getKeyspace() {
        return _keyspace;
    }

    ColumnFamily<ByteBuffer, Composite> getBlobColumnFamily() {
        return _blobColumnFamily;
    }

    // for debugging
    @Override
    public String toString() {
        return _name;
    }
}
