package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.api.*;
import com.bazaarvoice.emodb.sor.audit.s3.AthenaAuditWriter;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.table.db.TableBackingStore;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

public class WriteCloseableDataStore implements DataStore, TableBackingStore, Closeable {

    private final static Logger _log = LoggerFactory.getLogger(AthenaAuditWriter.class);

    private final DataStore _delegate;
    private final TableBackingStore _tableBackingStore;
    private final Phaser _writerPhaser;
    private volatile boolean _writesAccepted;

    @Inject
    public WriteCloseableDataStore(@ManagedDataStoreDelegate DataStore delegate,
                                   @ManagedTableBackingStoreDelegate TableBackingStore tableBackingStore,
                                   GracefulShutdownRegistry gracefulShutdownRegistry) {
        _delegate = requireNonNull(delegate);
        _tableBackingStore = requireNonNull(tableBackingStore);
        _writesAccepted = true;
        _writerPhaser = new Phaser();
        requireNonNull(gracefulShutdownRegistry).registerWriter(this);
    }

    @Override
    public void close() {
        _writesAccepted = false;
        try {
            _writerPhaser.awaitAdvanceInterruptibly(_writerPhaser.arrive(), 10, TimeUnit.SECONDS);
        } catch (InterruptedException | TimeoutException e) {
            _log.warn("Failed to shutdown writes fully, there are likely uncompleted writes.");
        }
    }

    @Override
    public Iterator<Table> listTables(@Nullable String fromTableExclusive, long limit) {
        return _delegate.listTables(fromTableExclusive, limit);
    }

    @Override
    public Iterator<UnpublishedDatabusEvent> listUnpublishedDatabusEvents(Date fromInclusive, Date toExclusive) {
        return _delegate.listUnpublishedDatabusEvents(fromInclusive, toExclusive);
    }

    @Override
    public void createTable(String table, TableOptions options, Map<String, ?> template, Audit audit) throws TableExistsException {
        executeIfAcceptingWrites(() -> _delegate.createTable(table, options, template, audit));
    }

    @Override
    public void dropTable(String table, Audit audit) throws UnknownTableException {
        executeIfAcceptingWrites(() -> _delegate.dropTable(table, audit));
    }

    @Override
    public void purgeTableUnsafe(String table, Audit audit) throws UnknownTableException {
        executeIfAcceptingWrites(() -> _delegate.purgeTableUnsafe(table, audit));
    }

    @Override
    public boolean getTableExists(String table) {
        return _delegate.getTableExists(table);
    }

    @Override
    public boolean isTableAvailable(String table) {
        return _delegate.isTableAvailable(table);
    }

    @Override
    public Table getTableMetadata(String table) {
        return _delegate.getTableMetadata(table);
    }

    @Override
    public Map<String, Object> getTableTemplate(String table) throws UnknownTableException {
        return _delegate.getTableTemplate(table);
    }

    @Override
    public void setTableTemplate(String table, Map<String, ?> template, Audit audit) throws UnknownTableException {
        executeIfAcceptingWrites(() -> _delegate.setTableTemplate(table, template, audit));
    }

    @Override
    public TableOptions getTableOptions(String table) throws UnknownTableException {
        return _delegate.getTableOptions(table);
    }

    @Override
    public long getTableApproximateSize(String table) throws UnknownTableException {
        return _delegate.getTableApproximateSize(table);
    }

    @Override
    public long getTableApproximateSize(String table, int limit) throws UnknownTableException {
        return _delegate.getTableApproximateSize(table, limit);
    }

    @Override
    public Map<String, Object> get(String table, String key) {
        return _delegate.get(table, key);
    }

    @Override
    public Map<String, Object> get(String table, String key, ReadConsistency consistency) {
        return _delegate.get(table, key, consistency);
    }

    @Override
    public Iterator<Map<String, Object>> scan(String table, @Nullable String fromKeyExclusive, LimitCounter limit, ReadConsistency consistency) {
        return _tableBackingStore.scan(table, fromKeyExclusive, limit, consistency);
    }

    @Override
    public Iterator<Change> getTimeline(String table, String key, boolean includeContentData, boolean includeAuditInformation, @Nullable UUID start, @Nullable UUID end, boolean reversed, long limit, ReadConsistency consistency) {
        return _delegate.getTimeline(table, key, includeContentData, includeAuditInformation, start, end, reversed, limit, consistency);
    }

    @Override
    public Iterator<Map<String, Object>> scan(String table, @Nullable String fromKeyExclusive, long limit, boolean includeDeletes, ReadConsistency consistency) {
        return _delegate.scan(table, fromKeyExclusive, limit, includeDeletes, consistency);
    }

    @Override
    public Collection<String> getSplits(String table, int desiredRecordsPerSplit) {
        return _delegate.getSplits(table, desiredRecordsPerSplit);
    }

    @Override
    public Iterator<Map<String, Object>> getSplit(String table, String split, @Nullable String fromKeyExclusive, long limit, boolean includeDeletes, ReadConsistency consistency) {
        return _delegate.getSplit(table, split, fromKeyExclusive, limit, includeDeletes, consistency);
    }

    @Override
    public Iterator<Map<String, Object>> multiGet(List<Coordinate> coordinates) {
        return _delegate.multiGet(coordinates);
    }

    @Override
    public Iterator<Map<String, Object>> multiGet(List<Coordinate> coordinates, ReadConsistency consistency) {
        return _delegate.multiGet(coordinates, consistency);
    }

    @Override
    public void update(String table, String key, UUID changeId, Delta delta, Audit audit) {
        executeIfAcceptingWrites(() -> _delegate.update(table, key, changeId, delta, audit));
    }

    @Override
    public void update(String table, String key, UUID changeId, Delta delta, Audit audit, WriteConsistency consistency) {
        executeIfAcceptingWrites(() -> _delegate.update(table, key, changeId, delta, audit, consistency));
    }

    @Override
    public void updateAll(Iterable<Update> updates) {
        updateAll(updates, ImmutableSet.of());

    }

    @Override
    public void updateAll(Iterable<Update> updates, Set<String> tags) {
        _writerPhaser.register();
        try {
            Iterator<Update> updateIterator = updates.iterator();
            _delegate.updateAll(closeableIterator(updateIterator), tags);
            if (updateIterator.hasNext()) {
                throw new ServiceUnavailableException();
            }
        } finally {
            _writerPhaser.arriveAndDeregister();
        }
    }

    @Override
    public void compact(String table, String key, @Nullable Duration ttlOverride, ReadConsistency readConsistency, WriteConsistency writeConsistency) {
        _delegate.compact(table, key, ttlOverride, readConsistency, writeConsistency);
    }

    @Override
    public Collection<String> getTablePlacements() {
        return _delegate.getTablePlacements();
    }

    @Override
    public void createFacade(String table, FacadeOptions options, Audit audit) throws TableExistsException {
        executeIfAcceptingWrites(() -> _delegate.createFacade(table, options, audit));
    }

    @Override
    public void updateAllForFacade(Iterable<Update> updates) {
        updateAllForFacade(updates, ImmutableSet.of());
    }

    @Override
    public void updateAllForFacade(Iterable<Update> updates, Set<String> tags) {
        _writerPhaser.register();
        try {
            Iterator<Update> updateIterator = updates.iterator();
            _delegate.updateAllForFacade(closeableIterator(updateIterator), tags);
            if (updateIterator.hasNext()) {
                throw new ServiceUnavailableException();
            }
        } finally {
            _writerPhaser.arriveAndDeregister();
        }
    }

    @Override
    public void dropFacade(String table, String placement, Audit audit) throws UnknownTableException {
        executeIfAcceptingWrites(() -> _delegate.dropFacade(table, placement, audit));
    }

    @Override
    public URI getStashRoot() throws StashNotAvailableException {
        return _delegate.getStashRoot();
    }

    private void executeIfAcceptingWrites(Runnable runnable) {
        _writerPhaser.register();
        try {
            if (_writesAccepted) {
                runnable.run();
            } else {
                throw new ServiceUnavailableException();
            }
        } finally {
            _writerPhaser.arriveAndDeregister();
        }
    }

    private Iterable<Update> closeableIterator(Iterator<Update> updates) {
        return new Iterable<Update>() {
            @Override
            public Iterator<Update> iterator() {
                return new AbstractIterator<Update>() {
                    @Override
                    protected Update computeNext() {
                        if (!updates.hasNext() || !_writesAccepted) {
                            return endOfData();
                        }
                        return updates.next();
                    }
                };
            }
        };
    }
}
