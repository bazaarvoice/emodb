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
import com.google.common.collect.Sets;
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
 * Supports delegation of DDL operations to the system data center.
 */
public class DataStoreProxy implements DataStore {

    private final DataStore _local;
    private final DataStore _system;

    public DataStoreProxy(DataStore local, DataStore system) {
        _local = local;
        _system = system;
    }

    @Override
    public Iterator<Table> listTables(@Nullable String fromTableExclusive, long limit) {
        return _local.listTables(fromTableExclusive, limit);
    }

    @Override
    public void createTable(String table, TableOptions options, Map<String, ?> template, Audit audit) throws TableExistsException {
        // delegate to system data center
        _system.createTable(table, options, template, audit);
    }

    @Override
    public void dropTable(String table, Audit audit) throws UnknownTableException {
        // delegate to system data center
        _system.dropTable(table, audit);
    }

    @Override
    public void purgeTableUnsafe(String table, Audit audit) throws UnknownTableException {
        _local.purgeTableUnsafe(table, audit);
    }

    @Override
    public boolean getTableExists(String table) {
        return _local.getTableExists(table);
    }

    @Override
    public boolean isTableAvailable(String table) {
        return _local.isTableAvailable(table);
    }

    @Override
    public Table getTableMetadata(String table) {
        return _local.getTableMetadata(table);
    }

    @Override
    public Map<String, Object> getTableTemplate(String table) throws UnknownTableException {
        return _local.getTableTemplate(table);
    }

    @Override
    public void setTableTemplate(String table, Map<String, ?> template, Audit audit) throws UnknownTableException {
        _local.setTableTemplate(table, template, audit);
    }

    @Override
    public TableOptions getTableOptions(String table) throws UnknownTableException {
        return _local.getTableOptions(table);
    }

    @Override
    public long getTableApproximateSize(String table) throws UnknownTableException {
        return _local.getTableApproximateSize(table);
    }

    @Override
    public long getTableApproximateSize(String table, int limit) throws UnknownTableException {
        return _local.getTableApproximateSize(table, limit);
    }

    @Override
    public Map<String, Object> get(String table, String key) {
        return _local.get(table, key);
    }

    @Override
    public Map<String, Object> get(String table, String key, ReadConsistency consistency) {
        return _local.get(table, key, consistency);
    }

    @Override
    public Iterator<Change> getTimeline(String table, String key, boolean includeContentData, boolean includeAuditInformation, @Nullable UUID start, @Nullable UUID end, boolean reversed, long limit, ReadConsistency consistency) {
        return _local.getTimeline(table, key, includeContentData, includeAuditInformation, start, end, reversed, limit, consistency);
    }

    @Override
    public Iterator<Map<String, Object>> scan(String table, @Nullable String fromKeyExclusive, long limit, ReadConsistency consistency) {
        return _local.scan(table, fromKeyExclusive, limit, consistency);
    }

    @Override
    public Collection<String> getSplits(String table, int desiredRecordsPerSplit) {
        return _local.getSplits(table, desiredRecordsPerSplit);
    }

    @Override
    public Iterator<Map<String, Object>> getSplit(String table, String split, @Nullable String fromKeyExclusive, long limit, ReadConsistency consistency) {
        return _local.getSplit(table, split, fromKeyExclusive, limit, consistency);
    }

    @Override
    public Iterator<Map<String, Object>> multiGet(List<Coordinate> coordinates) {
        return _local.multiGet(coordinates);
    }

    @Override
    public Iterator<Map<String, Object>> multiGet(List<Coordinate> coordinates, ReadConsistency consistency) {
        return _local.multiGet(coordinates, consistency);
    }

    @Override
    public void update(String table, String key, UUID changeId, Delta delta, Audit audit) {
        _local.update(table, key, changeId, delta, audit);
    }

    @Override
    public void update(String table, String key, UUID changeId, Delta delta, Audit audit, WriteConsistency consistency) {
        _local.update(table, key, changeId, delta, audit, consistency);
    }

    @Override
    public void updateAll(Iterable<Update> updates) {
        _local.updateAll(updates);
    }

    @Override
    public void updateAll(Iterable<Update> updates, Set<String> tags) {
        _local.updateAll(updates, tags);
    }

    @Override
    public void compact(String table, String key, @Nullable Duration ttlOverride, ReadConsistency readConsistency, WriteConsistency writeConsistency) {
        _local.compact(table, key, ttlOverride, readConsistency, writeConsistency);
    }

    @Override
    public Collection<String> getTablePlacements() {
        return _local.getTablePlacements();
    }

    @Override
    public void createFacade(String table, FacadeOptions options, Audit audit) throws TableExistsException {
        // delegate to system data center
        _system.createFacade(table, options, audit);
    }

    @Override
    public void updateAllForFacade(Iterable<Update> updates) {
        _local.updateAllForFacade(updates);
    }

    @Override
    public void updateAllForFacade(Iterable<Update> updates, Set<String> tags) {
        _local.updateAllForFacade(updates, tags);
    }

    @Override
    public void dropFacade(String table, String placement, Audit audit) throws UnknownTableException {
        // delegate to system data center
        _system.dropFacade(table, placement, audit);
    }

    @Override
    public URI getStashRoot() throws StashNotAvailableException {
        return _local.getStashRoot();
    }
}
