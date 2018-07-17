package com.bazaarvoice.emodb.blob.core;

import com.bazaarvoice.emodb.blob.api.Blob;
import com.bazaarvoice.emodb.blob.api.BlobMetadata;
import com.bazaarvoice.emodb.blob.api.BlobNotFoundException;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.RangeNotSatisfiableException;
import com.bazaarvoice.emodb.blob.api.RangeSpecification;
import com.bazaarvoice.emodb.blob.api.Table;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.io.InputSupplier;
import com.google.inject.Inject;
import com.google.inject.Provider;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Supports delegation of DDL operations to the system data center.  This implementation uses a Providers for
 * the blob store injection to prevent re-entrant injection issues.
 */
public class BlobStoreProviderProxy implements BlobStore {

    private final Supplier<BlobStore> _local;
    private final Supplier<BlobStore> _system;

    @Inject
    public BlobStoreProviderProxy(@LocalBlobStore Provider<BlobStore> local, @SystemBlobStore Provider<BlobStore> system) {
        // The providers should be singletons.  Even so, locally memoize to ensure use of a singleton.
        _local = Suppliers.memoize(local::get);
        _system = Suppliers.memoize(system::get);
    }

    // Calls which modify the blob store DDL must be redirected to the system data center

    @Override
    public void createTable(String table, TableOptions options, Map<String, String> attributes, Audit audit)
            throws TableExistsException {
        _system.get().createTable(table, options, attributes, audit);
    }

    @Override
    public void dropTable(String table, Audit audit) throws UnknownTableException {
        _system.get().dropTable(table, audit);
    }

    @Override
    public void setTableAttributes(String table, Map<String, String> attributes, Audit audit)
            throws UnknownTableException {
        _system.get().setTableAttributes(table, attributes, audit);
    }

    // All other calls can be serviced locally

    @Override
    public Iterator<Table> listTables(@Nullable String fromTableExclusive, long limit) {
        return _local.get().listTables(fromTableExclusive, limit);
    }

    @Override
    public void purgeTableUnsafe(String table, Audit audit) throws UnknownTableException {
        _local.get().purgeTableUnsafe(table, audit);
    }

    @Override
    public boolean getTableExists(String table) {
        return _local.get().getTableExists(table);
    }

    @Override
    public boolean isTableAvailable(String table) {
        return _local.get().isTableAvailable(table);
    }

    @Override
    public Table getTableMetadata(String table) {
        return _local.get().getTableMetadata(table);
    }

    @Override
    public Map<String, String> getTableAttributes(String table) throws UnknownTableException {
        return _local.get().getTableAttributes(table);
    }

    @Override
    public TableOptions getTableOptions(String table) throws UnknownTableException {
        return _local.get().getTableOptions(table);
    }

    @Override
    public long getTableApproximateSize(String table) throws UnknownTableException {
        return _local.get().getTableApproximateSize(table);
    }

    @Override
    public BlobMetadata getMetadata(String table, String blobId) throws BlobNotFoundException {
        return _local.get().getMetadata(table, blobId);
    }

    @Override
    public Iterator<BlobMetadata> scanMetadata(String table, @Nullable String fromBlobIdExclusive, long limit) {
        return _local.get().scanMetadata(table, fromBlobIdExclusive, limit);
    }

    @Override
    public Blob get(String table, String blobId) throws BlobNotFoundException {
        return _local.get().get(table, blobId);
    }

    @Override
    public Blob get(String table, String blobId, @Nullable RangeSpecification rangeSpec)
            throws BlobNotFoundException, RangeNotSatisfiableException {
        return _local.get().get(table, blobId, rangeSpec);
    }

    @Override
    public void put(String table, String blobId, InputSupplier<? extends InputStream> in, Map<String, String> attributes, @Nullable Duration ttl)
            throws IOException {
        _local.get().put(table, blobId, in, attributes, ttl);
    }

    @Override
    public void delete(String table, String blobId) {
        _local.get().delete(table, blobId);
    }

    @Override
    public Collection<String> getTablePlacements() {
        return _local.get().getTablePlacements();
    }
}
