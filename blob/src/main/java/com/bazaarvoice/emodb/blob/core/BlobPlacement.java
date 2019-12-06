package com.bazaarvoice.emodb.blob.core;

import com.amazonaws.services.s3.AmazonS3;
import com.bazaarvoice.emodb.blob.db.s3.S3BucketConfiguration;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.table.db.astyanax.Placement;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Composite;

import java.nio.ByteBuffer;
import java.util.Objects;


/**
 * A Cassandra keyspace and the BlobStore column family.
 */
public class BlobPlacement implements Placement {
    private final String _name;
    private final CassandraKeyspace _keyspace;
    private final ColumnFamily<ByteBuffer, Composite> _blobColumnFamily;
    private final S3BucketConfiguration _s3BucketConfiguration;
    private final AmazonS3 _s3Client;

    BlobPlacement(String name,
                  CassandraKeyspace keyspace,
                  ColumnFamily<ByteBuffer, Composite> blobColumnFamily,
                  S3BucketConfiguration s3BucketConfiguration,
                  AmazonS3 s3Client) {
        _name = Objects.requireNonNull(name, "name");
        _keyspace = Objects.requireNonNull(keyspace, "keyspace");
        _blobColumnFamily = Objects.requireNonNull(blobColumnFamily, "blobColumnFamily");
        //TODO add Objects.requireNonNull in EMO-7107
        _s3BucketConfiguration = s3BucketConfiguration;
        _s3Client = s3Client;
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public CassandraKeyspace getKeyspace() {
        return _keyspace;
    }

    public ColumnFamily<ByteBuffer, Composite> getBlobColumnFamily() {
        return _blobColumnFamily;
    }

    public S3BucketConfiguration getS3BucketConfiguration() {
        return _s3BucketConfiguration;
    }

    public AmazonS3 getS3Client() {
        return _s3Client;
    }

    @Override
    public String toString() {
        return _name;
    }
}
