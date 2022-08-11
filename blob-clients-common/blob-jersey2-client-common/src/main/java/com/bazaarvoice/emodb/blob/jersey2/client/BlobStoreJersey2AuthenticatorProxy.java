package com.bazaarvoice.emodb.blob.jersey2.client;

import com.bazaarvoice.emodb.auth.util.ApiKeyEncryption;
import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
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

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

/**
 * BlobStore instance that takes an {@link AuthBlobStore} and API key and proxies all calls using the API key.
 */
class BlobStoreJersey2AuthenticatorProxy implements BlobStore {

    private final AuthBlobStore _authBlobStore;
    private final String _apiKey;

    BlobStoreJersey2AuthenticatorProxy(AuthBlobStore authBlobStore, String apiKey) {
        _authBlobStore = authBlobStore;
        if(ApiKeyEncryption.isPotentiallyEncryptedApiKey(apiKey)){
            throw new IllegalArgumentException("API Key is encrypted, please decrypt it");
        }else {
            _apiKey = apiKey;
        }
    }

    @Override
    public Iterator<Table> listTables(@Nullable String fromTableExclusive, long limit) {
        return _authBlobStore.listTables(_apiKey, fromTableExclusive, limit);
    }

    @Override
    public Iterator<BlobMetadata> scanMetadata(String table, @Nullable String fromBlobIdExclusive, long limit) {
        return _authBlobStore.scanMetadata(_apiKey, table, fromBlobIdExclusive, limit);
    }

    @Override
    public TableOptions getTableOptions(String table) throws UnknownTableException {
        return _authBlobStore.getTableOptions(_apiKey, table);
    }

    @Override
    public Blob get(String table, String blobId) throws BlobNotFoundException {
        return _authBlobStore.get(_apiKey, table, blobId);
    }

    @Override
    public boolean getTableExists(String table) {
        return _authBlobStore.getTableExists(_apiKey, table);
    }

    @Override
    public Table getTableMetadata(String table) {
        return _authBlobStore.getTableMetadata(_apiKey, table);
    }

    @Override
    public BlobMetadata getMetadata(String table, String blobId)
            throws BlobNotFoundException {
        return _authBlobStore.getMetadata(_apiKey, table, blobId);
    }

    @Override
    public void dropTable(String table, Audit audit) throws UnknownTableException {
        _authBlobStore.dropTable(_apiKey, table, audit);
    }

    @Override
    public Collection<String> getTablePlacements() {
        return _authBlobStore.getTablePlacements(_apiKey);
    }

    @Override
    public void put(String table, String blobId, Supplier<? extends InputStream> in, Map<String, String> attributes)
            throws IOException {
        _authBlobStore.put(_apiKey, table, blobId, in, attributes);
    }

    @Override
    public void delete(String table, String blobId) {
        _authBlobStore.delete(_apiKey, table, blobId);
    }

    @Override
    public long getTableApproximateSize(String table) throws UnknownTableException {
        return _authBlobStore.getTableApproximateSize(_apiKey, table);
    }

    @Override
    public void purgeTableUnsafe(String table, Audit audit) throws UnknownTableException {
        _authBlobStore.purgeTableUnsafe(_apiKey, table, audit);
    }

    @Override
    public Blob get(String table, String blobId, @Nullable RangeSpecification rangeSpec)
            throws BlobNotFoundException, RangeNotSatisfiableException {
        return _authBlobStore.get(_apiKey, table, blobId, rangeSpec);
    }

    @Override
    public boolean isTableAvailable(String table) {
        return _authBlobStore.isTableAvailable(_apiKey, table);
    }

    @Override
    public Map<String, String> getTableAttributes(String table)
            throws UnknownTableException {
        return _authBlobStore.getTableAttributes(_apiKey, table);
    }

    @Override
    public void createTable(String table, TableOptions options, Map<String, String> attributes, Audit audit)
            throws TableExistsException {
        _authBlobStore.createTable(_apiKey, table, options, attributes, audit);
    }

    @Override
    public void setTableAttributes(String table, Map<String, String> attributes, Audit audit)
            throws UnknownTableException {
        _authBlobStore.setTableAttributes(_apiKey, table, attributes, audit);
    }

    AuthBlobStore getProxiedInstance() {
        return _authBlobStore;
    }
}
