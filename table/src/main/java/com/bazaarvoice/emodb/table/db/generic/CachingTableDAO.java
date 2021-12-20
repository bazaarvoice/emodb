package com.bazaarvoice.emodb.table.db.generic;

import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.common.dropwizard.time.ClockTicker;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.FacadeExistsException;
import com.bazaarvoice.emodb.sor.api.FacadeOptions;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnknownFacadeException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.api.UnpublishedDatabusEvent;
import com.bazaarvoice.emodb.sor.api.UnpublishedDatabusEventType;
import com.bazaarvoice.emodb.table.db.DroppedTableException;
import com.bazaarvoice.emodb.table.db.MoveType;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Wraps a {@link TableDAO} with a cache that makes it fast and efficient to lookup table metadata.  The downside
 * is that servers must globally coordinate changes to table metadata because the consequences of using out-of-date
 * cached table metadata are pretty severe.
 * <p>
 * This class assumes that create/drop operations are protected/wrapped by {@link MutexTableDAO}.
 */
public class CachingTableDAO implements TableDAO {
    // If a table is unknown cache that for a much shorter time to minimize the effect of invalidation delays
    public static final Duration UNKNOWN_TABLE_RELOAD_DURATION = Duration.ofSeconds(2);
    public static final Duration CACHE_DURATION = Duration.ofMinutes(10);

    private final TableDAO _delegate;
    private final LoadingCache<String, TableCacheEntry> _tableCache;
    private final Clock _clock;

    @Inject
    public CachingTableDAO(@CachingTableDAODelegate TableDAO delegate,
                           @CachingTableDAORegistry CacheRegistry cacheRegistry,
                           Clock clock) {
        _delegate = requireNonNull(delegate, "delegate");
        _clock = requireNonNull(clock, "clock");

        // The table cache maps table names to AstyanaxTable objects.
        _tableCache = CacheBuilder.newBuilder()
                .ticker(ClockTicker.getTicker(clock))
                .expireAfterWrite(CACHE_DURATION.toMillis(), TimeUnit.MILLISECONDS)
                .recordStats()
                .build(new CacheLoader<String, TableCacheEntry>() {
                    @Override
                    public TableCacheEntry load(String name)
                            throws Exception {
                        try {
                            return new TableCacheEntry(_delegate.get(name));
                        } catch (UnknownTableException e) {
                            return new TableCacheEntry(_clock.instant().plus(UNKNOWN_TABLE_RELOAD_DURATION));
                        }
                    }
                });
        cacheRegistry.register("tables", _tableCache, true);
    }

    @Override
    public Iterator<Table> list(@Nullable String fromNameExclusive, LimitCounter limit) {
        return _delegate.list(fromNameExclusive, limit);
    }

    @Override
    public void writeUnpublishedDatabusEvent(String name, UnpublishedDatabusEventType attribute){
        requireNonNull(name, "table");

        _delegate.writeUnpublishedDatabusEvent(name, attribute);
    }

    @Override
    public Iterator<UnpublishedDatabusEvent> listUnpublishedDatabusEvents(Date fromInclusive, Date toExclusive) {
        return _delegate.listUnpublishedDatabusEvents(fromInclusive, toExclusive);
    }

    @Timed (name = "bv.emodb.table.CachingTableDAO.create", absolute = true)
    @Override
    public void create(String name, TableOptions options, Map<String, ?> attributes, Audit audit)
            throws TableExistsException {
        requireNonNull(name, "table");

        _delegate.create(name, options, attributes, audit);
    }

    @Override
    public void createFacade(String name, FacadeOptions options, Audit audit)
            throws FacadeExistsException {
        requireNonNull(name, "table");

        _delegate.createFacade(name, options, audit);
    }

    @Override
    public boolean checkFacadeAllowed(String name, FacadeOptions options)
            throws TableExistsException {
        return _delegate.checkFacadeAllowed(name, options);
    }

    @Timed (name = "bv.emodb.table.CachingTableDAO.drop", absolute = true)
    @Override
    public void drop(String name, Audit audit)
            throws UnknownTableException {
        requireNonNull(name, "table");

        _delegate.drop(name, audit);
    }

    @Override
    public void dropFacade(String name, String placement, Audit audit)
            throws UnknownFacadeException {
        requireNonNull(name, "table");

        _delegate.dropFacade(name, placement, audit);
    }

    @Override
    public void move(String name, String destPlacement, Optional<Integer> numShards, Audit audit, MoveType moveType)
            throws UnknownTableException {
        requireNonNull(name, "table");

        _delegate.move(name, destPlacement, numShards, audit, moveType);
    }

    @Override
    public void moveFacade(String name, String sourcePlacement, String destPlacement, Optional<Integer> numShards, Audit audit, MoveType moveType)
            throws UnknownTableException {
        requireNonNull(name, "table");

        _delegate.moveFacade(name, sourcePlacement, destPlacement, numShards, audit, moveType);
    }

    @Timed (name = "bv.emodb.table.CachingTableDAO.setAttributes", absolute = true)
    @Override
    public void setAttributes(String name, Map<String, ?> attributes, Audit audit)
            throws UnknownTableException {
        requireNonNull(name, "table");

        _delegate.setAttributes(name, attributes, audit);
    }

    @Override
    public void audit(String name, String op, Audit audit) {
        _delegate.audit(name, op, audit);
    }

    @Nullable
    private Table getTableFromCache(String name) {
        TableCacheEntry entry = _tableCache.getUnchecked(name);
        // If the table wasn't found and it is after the unknown table expiration time invalidate the cache and
        // load it again.
        if (entry.table == null && _clock.instant().isAfter(entry.unknownTableReloadTime)) {
            _tableCache.invalidate(name);
            entry = _tableCache.getUnchecked(name);
        }
        return entry.table;
    }

    @Override
    public boolean exists(String name) {
        return getTableFromCache(name) != null;
    }

    @Override
    public boolean isMoveToThisPlacementAllowed(String placement) {
        return _delegate.isMoveToThisPlacementAllowed(placement);
    }

    @Override
    public Table get(String name)
            throws UnknownTableException {
        Table table = getTableFromCache(name);
        if (table == null) {
            throw new UnknownTableException(format("Unknown table: %s", name), name);
        }
        return table;
    }

    @Override
    public Table getByUuid(long uuid)
            throws UnknownTableException, DroppedTableException {
        // ID lookups are done used only by internal tools.  Do not cache table information by ID.
        return _delegate.getByUuid(uuid);
    }

    @Override
    public Collection<String> getTablePlacements(boolean includeInternal, boolean localOnly) {
        return _delegate.getTablePlacements(includeInternal, localOnly);
    }

    @Override
    public TableSet createTableSet() {
        return _delegate.createTableSet();
    }

    private class TableCacheEntry {
        final Table table;
        final Instant unknownTableReloadTime;

        TableCacheEntry(Table table) {
            this.table = table;
            unknownTableReloadTime = Instant.MAX;
        }

        TableCacheEntry(Instant unknownTableReloadTime) {
            this.unknownTableReloadTime = unknownTableReloadTime;
            table = null;
        }
    }
}
