package com.bazaarvoice.emodb.sor.client2;

import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.FacadeOptions;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.StashNotAvailableException;
import com.bazaarvoice.emodb.sor.api.Table;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.api.UnpublishedDatabusEvent;
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Delta;

import javax.annotation.Nullable;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * DataStore instance that takes an {@link AuthDataStore} and API key and proxies all calls using the API key.
 */

class DataStoreAuthenticatorProxy implements DataStore {
    
    private final AuthDataStore _authDataStore;
    private final String _apiKey;

    DataStoreAuthenticatorProxy(AuthDataStore authDataStore, String apiKey) {
        _authDataStore = authDataStore;
        _apiKey = apiKey;
    }

    @Override
    public Iterator<Table> listTables(@Nullable String fromTableExclusive, long limit) {
        return _authDataStore.listTables(_apiKey, fromTableExclusive, limit);
    }

    @Override
    public Iterator<UnpublishedDatabusEvent> listUnpublishedDatabusEvents(Date fromInclusive, Date toExclusive) {
        return _authDataStore.listUnpublishedDatabusEvents(_apiKey, fromInclusive, toExclusive);
    }

    @Override
    public Collection<String> getTablePlacements() {
        return _authDataStore.getTablePlacements(_apiKey);
    }

    @Override
    public void updateAll(Iterable<Update> updates) {
        _authDataStore.updateAll(_apiKey, updates);
    }

    @Override
    public void updateAll(Iterable<Update> updates, Set<String> tags) {
        _authDataStore.updateAll(_apiKey, updates, tags);
    }

    @Override
    public Map<String, Object> getTableTemplate(String table) throws UnknownTableException {
        return _authDataStore.getTableTemplate(_apiKey, table);
    }

    @Override
    public void purgeTableUnsafe(String table, Audit audit) throws UnknownTableException {
        _authDataStore.purgeTableUnsafe(_apiKey, table, audit);
    }

    @Override
    public Collection<String> getSplits(String table, int desiredRecordsPerSplit) {
        return _authDataStore.getSplits(_apiKey, table, desiredRecordsPerSplit);
    }

    @Override
    public Map<String, Object> get(String table, String key) {
        return _authDataStore.get(_apiKey, table, key);
    }

    @Override
    public boolean getTableExists(String table) {
        return _authDataStore.getTableExists(_apiKey, table);
    }

    @Override
    public void dropTable(String table, Audit audit) throws UnknownTableException {
        _authDataStore.dropTable(_apiKey, table, audit);
    }

    @Override
    public long getTableApproximateSize(String table) throws UnknownTableException {
        return _authDataStore.getTableApproximateSize(_apiKey, table);
    }

    @Override
    public void update(String table, String key, UUID changeId, Delta delta, Audit audit) {
        _authDataStore.update(_apiKey, table, key, changeId, delta, audit);
    }

    @Override
    public void createTable(String table, TableOptions options, Map<String, ?> template, Audit audit)
            throws TableExistsException {
        _authDataStore.createTable(_apiKey, table, options, template, audit);
    }

    @Override
    public TableOptions getTableOptions(String table) throws UnknownTableException {
        return _authDataStore.getTableOptions(_apiKey, table);
    }

    @Override
    public Map<String, Object> get(String table, String key, ReadConsistency consistency) {
        return _authDataStore.get(_apiKey, table, key, consistency);
    }

    @Override
    public void update(String table, String key, UUID changeId, Delta delta, Audit audit, WriteConsistency consistency) {
        _authDataStore.update(_apiKey, table, key, changeId, delta, audit, consistency);
    }

    @Override
    public Iterator<Change> getTimeline(String table, String key, boolean includeContentData, boolean includeAuditInformation, @Nullable UUID start, @Nullable UUID end, boolean reversed, long limit, ReadConsistency consistency) {
        return _authDataStore.getTimeline(_apiKey, table, key, includeContentData, includeAuditInformation, start, end, reversed, limit, consistency);
    }

    @Override
    public void createFacade(String table, FacadeOptions options, Audit audit)
            throws TableExistsException {
        _authDataStore.createFacade(_apiKey, table, options, audit);
    }

    @Override
    public URI getStashRoot() throws StashNotAvailableException {
        return _authDataStore.getStashRoot(_apiKey);
    }

    @Override
    public void dropFacade(String table, String placement, Audit audit)
            throws UnknownTableException {
        _authDataStore.dropFacade(_apiKey, table, placement, audit);
    }

    @Override
    public boolean isTableAvailable(String table) {
        return _authDataStore.isTableAvailable(_apiKey, table);
    }

    @Override
    public Iterator<Map<String, Object>> scan(String table, @Nullable String fromKeyExclusive, long limit, boolean includeDeletes, ReadConsistency consistency) {
        return _authDataStore.scan(_apiKey, table, fromKeyExclusive, limit, includeDeletes, consistency);
    }

    @Override
    public Iterator<Map<String, Object>> getSplit(String table, String split, @Nullable String fromKeyExclusive, long limit, boolean includeDeletes, ReadConsistency consistency) {
        return _authDataStore.getSplit(_apiKey, table, split, fromKeyExclusive, limit, includeDeletes, consistency);
    }

    @Override
    public Iterator<Map<String, Object>> multiGet(List<Coordinate> coordinates) {
        return _authDataStore.multiGet(_apiKey, coordinates);
    }

    @Override
    public Iterator<Map<String, Object>> multiGet(List<Coordinate> coordinates, ReadConsistency consistency) {
        return _authDataStore.multiGet(_apiKey, coordinates, consistency);
    }

    @Override
    public void compact(String table, String key, @Nullable Duration ttlOverride, ReadConsistency readConsistency, WriteConsistency writeConsistency) {
        _authDataStore.compact(_apiKey, table, key, ttlOverride, readConsistency, writeConsistency);
    }

    @Override
    public long getTableApproximateSize(String table, int limit)
            throws UnknownTableException {
        return _authDataStore.getTableApproximateSize(_apiKey, table, limit);
    }

    @Override
    public void updateAllForFacade(Iterable<Update> updates) {
        _authDataStore.updateAllForFacade(_apiKey, updates);
    }

    @Override
    public void updateAllForFacade(Iterable<Update> updates, Set<String> tags) {
        _authDataStore.updateAllForFacade(_apiKey, updates, tags);
    }

    @Override
    public void setTableTemplate(String table, Map<String, ?> template, Audit audit)
            throws UnknownTableException {
        _authDataStore.setTableTemplate(_apiKey, table, template, audit);
    }

    @Override
    public Table getTableMetadata(String table) {
        return _authDataStore.getTableMetadata(_apiKey, table);
    }

    AuthDataStore getProxiedInstance() {
        return _authDataStore;
    }
}
