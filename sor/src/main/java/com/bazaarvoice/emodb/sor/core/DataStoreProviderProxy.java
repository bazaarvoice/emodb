package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.sor.api.Audit;
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
import com.bazaarvoice.emodb.sor.api.Update;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Supports delegation of DDL operations to the system data center.  This implementation uses a Providers for
 * the data stores to prevent re-entrant injection issues.
 */
public class DataStoreProviderProxy implements DataStore {

    private final Provider<DataStore> _local;
    private final Provider<DataStore> _system;

    @Inject
    public DataStoreProviderProxy(@LocalDataStore Provider<DataStore> local, @SystemDataStore Provider<DataStore> system) {
        // The underlying providers use singletons so using Provider.get() in the implementation methods should be efficient
        _local = local;
        _system = system;
    }

    @Override
    public Iterator<Table> listTables(@Nullable String fromTableExclusive, long limit) {
        return _local.get().listTables(fromTableExclusive, limit);
    }

    @Override
    public void createTable(String table, TableOptions options, Map<String, ?> template, Audit audit) throws TableExistsException {
        // delegate to system data center
        _system.get().createTable(table, options, template, audit);
    }

    @Override
    public void dropTable(String table, Audit audit) throws UnknownTableException {
        // delegate to system data center
        _system.get().dropTable(table, audit);
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
    public Map<String, Object> getTableTemplate(String table) throws UnknownTableException {
        return _local.get().getTableTemplate(table);
    }

    @Override
    public void setTableTemplate(String table, Map<String, ?> template, Audit audit) throws UnknownTableException {
        _local.get().setTableTemplate(table, template, audit);
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
    public long getTableApproximateSize(String table, int limit) throws UnknownTableException {
        return _local.get().getTableApproximateSize(table, limit);
    }

    @Override
    public Map<String, Object> get(String table, String key) {
        return _local.get().get(table, key);
    }

    @Override
    public Map<String, Object> get(String table, String key, ReadConsistency consistency) {
        return _local.get().get(table, key, consistency);
    }

    @Override
    public Iterator<Change> getTimeline(String table, String key, boolean includeContentData, boolean includeAuditInformation, @Nullable UUID start, @Nullable UUID end, boolean reversed, long limit, ReadConsistency consistency) {
        return _local.get().getTimeline(table, key, includeContentData, includeAuditInformation, start, end, reversed, limit, consistency);
    }

    @Override
    public Iterator<Map<String, Object>> scan(String table, @Nullable String fromKeyExclusive, long limit, ReadConsistency consistency) {
        return _local.get().scan(table, fromKeyExclusive, limit, consistency);
    }

    @Override
    public Collection<String> getSplits(String table, int desiredRecordsPerSplit) {
        return _local.get().getSplits(table, desiredRecordsPerSplit);
    }

    @Override
    public Iterator<Map<String, Object>> getSplit(String table, String split, @Nullable String fromKeyExclusive, long limit, ReadConsistency consistency) {
        return _local.get().getSplit(table, split, fromKeyExclusive, limit, consistency);
    }

    @Override
    public Iterator<Map<String, Object>> multiGet(List<Coordinate> coordinates) {
        return _local.get().multiGet(coordinates);
    }

    @Override
    public Iterator<Map<String, Object>> multiGet(List<Coordinate> coordinates, ReadConsistency consistency) {
        return _local.get().multiGet(coordinates, consistency);
    }

    @Override
    public void update(String table, String key, UUID changeId, Delta delta, Audit audit) {
        _local.get().update(table, key, changeId, delta, audit);
    }

    @Override
    public void update(String table, String key, UUID changeId, Delta delta, Audit audit, WriteConsistency consistency) {
        _local.get().update(table, key, changeId, delta, audit, consistency);
    }

    @Override
    public void updateAll(Iterable<Update> updates) {
        _local.get().updateAll(updates);
    }

    @Override
    public void updateAll(Iterable<Update> updates, Set<String> tags) {
        _local.get().updateAll(updates, tags);
    }

    @Override
    public void compact(String table, String key, @Nullable Duration ttlOverride, ReadConsistency readConsistency, WriteConsistency writeConsistency) {
        _local.get().compact(table, key, ttlOverride, readConsistency, writeConsistency);
    }

    @Override
    public Collection<String> getTablePlacements() {
        return _local.get().getTablePlacements();
    }

    @Override
    public void createFacade(String table, FacadeOptions options, Audit audit) throws TableExistsException {
        // delegate to system data center
        _system.get().createFacade(table, options, audit);
    }

    @Override
    public void updateAllForFacade(Iterable<Update> updates) {
        _local.get().updateAllForFacade(updates);
    }

    @Override
    public void updateAllForFacade(Iterable<Update> updates, Set<String> tags) {
        _local.get().updateAllForFacade(updates, tags);
    }

    @Override
    public void dropFacade(String table, String placement, Audit audit) throws UnknownTableException {
        // delegate to system data center
        _system.get().dropFacade(table, placement, audit);
    }

    @Override
    public URI getStashRoot() throws StashNotAvailableException {
        return _local.get().getStashRoot();
    }
}
