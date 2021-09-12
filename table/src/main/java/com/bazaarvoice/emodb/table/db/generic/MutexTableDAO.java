package com.bazaarvoice.emodb.table.db.generic;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
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
import com.bazaarvoice.emodb.table.db.curator.TableMutexManager;
import com.google.common.base.Optional;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps a {@link TableDAO} with a global mutex that serializes create and drop table operations across the entire
 * system.
 */
public class MutexTableDAO implements TableDAO {
    public static final Duration ACQUIRE_TIMEOUT = Duration.ofSeconds(5);

    private final TableDAO _delegate;
    private final Optional<TableMutexManager> _mutexManager;

    @Inject
    public MutexTableDAO(@MutexTableDAODelegate TableDAO delegate, Optional<TableMutexManager> mutexManager) {
        _delegate = checkNotNull(delegate, "delegate");
        _mutexManager = checkNotNull(mutexManager, "table mutex manager");
    }

    @Override
    public Iterator<Table> list(@Nullable String fromNameExclusive, LimitCounter limit) {
        return _delegate.list(fromNameExclusive, limit);
    }

    @Override
    public void writeUnpublishedDatabusEvent(String name, UnpublishedDatabusEventType attribute){
        checkNotNull(name, "table");
        _delegate.writeUnpublishedDatabusEvent(name, attribute);
    }

    @Override
    public Iterator<UnpublishedDatabusEvent> listUnpublishedDatabusEvents(Date fromInclusive, Date toExclusive) {
        return _delegate.listUnpublishedDatabusEvents(fromInclusive, toExclusive);
    }

    @Override
    public void create(final String name, final TableOptions options, final Map<String, ?> attributes, final Audit audit)
            throws TableExistsException {
        // Optimistically skip creating the table if it exists already so we don't thrash the ZooKeeper lock if clients
        // call create table repeatedly.  If the table doesn't exist or is different, AstyanaxTableDAO will do this
        // check again for safety after obtaining the data-center-wide ZooKeeper lock.
        Table existingTable;
        try {
            existingTable = _delegate.get(name);
            if (existingTable.getOptions().getPlacement().equals(options.getPlacement())
                    && existingTable.getAttributes().equals(attributes)) {
                return;  // Nothing to do
            }
        } catch (UnknownTableException ute) {
            // Ignore
        }

        withLock(new Runnable() {
            @Override
            public void run() {
                _delegate.create(name, options, attributes, audit);
            }
        }, name);
    }

    @Override
    public void createFacade(final String name, final FacadeOptions options, final Audit audit)
            throws FacadeExistsException {
        if (!checkFacadeAllowed(name, options)) {
            return;
        }

        withLock(new Runnable() {
            @Override
            public void run() {
                _delegate.createFacade(name, options, audit);
            }
        }, name);
    }

    @Override
    public boolean checkFacadeAllowed(String name, FacadeOptions options)
            throws TableExistsException {
        return _delegate.checkFacadeAllowed(name, options);
    }

    @Override
    public void drop(final String name, final Audit audit)
            throws UnknownTableException {
        withLock(new Runnable() {
            @Override
            public void run() {
                _delegate.drop(name, audit);
            }
        }, name);
    }

    @Override
    public void dropFacade(final String name, final String placement, final Audit audit)
            throws UnknownFacadeException {
        withLock(new Runnable() {
            @Override
            public void run() {
                _delegate.dropFacade(name, placement, audit);
            }
        }, name);
    }

    @Override
    public void move(final String name, final String destPlacement,
                     final Optional<Integer> numShards, final Audit audit, final MoveType moveType)
            throws UnknownTableException {
        withLock(new Runnable() {
            @Override
            public void run() {
                _delegate.move(name, destPlacement, numShards, audit, moveType);
            }
        }, name);
    }

    @Override
    public void moveFacade(final String name, final String sourcePlacement, final String destPlacement,
                           final Optional<Integer> numShards, final Audit audit, final MoveType moveType)
            throws UnknownTableException {
        withLock(new Runnable() {
            @Override
            public void run() {
                _delegate.moveFacade(name, sourcePlacement, destPlacement, numShards, audit, moveType);
            }
        }, name);
    }

    @Override
    public void setAttributes(final String name, final Map<String, ?> attributes, final Audit audit)
            throws UnknownTableException {
        // Optimistically skip updating the table if nothing will change so we don't thrash the ZooKeeper lock if
        // clients call create table repeatedly.  If the table doesn't exist, the call will fail immediately.
        if (_delegate.get(name).getAttributes().equals(attributes)) {
            return;  // Nothing to do
        }

        // Obtain the global lock so this doesn't race concurrent create()/drop() operations.
        withLock(new Runnable() {
            @Override
            public void run() {
                _delegate.setAttributes(name, attributes, audit);
            }
        }, name);
    }

    @Override
    public void audit(String name, String op, Audit audit) {
        _delegate.audit(name, op, audit);
    }

    @Override
    public boolean exists(String name) {
        return _delegate.exists(name);
    }

    @Override
    public boolean isMoveToThisPlacementAllowed(String placement) {
        return _delegate.isMoveToThisPlacementAllowed(placement);
    }

    @Override
    public Table get(String name)
            throws UnknownTableException {
        return _delegate.get(name);
    }

    @Override
    public Table getByUuid(long uuid)
            throws UnknownTableException, DroppedTableException {
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

    private void withLock(Runnable runnable, String table) {
        if (!_mutexManager.isPresent()) {
            throw new UnsupportedOperationException(
                    "The table metadata mutex is unavailable from this data center. " +
                            "Make sure that the `systemDataCenter` property points to the right system datacenter. " +
                            "If this is a new data center and not the system datacenter, then try repairing the new Cassandra cluster as it may not have all " +
                            "the system tables replicated yet.");
        }
        _mutexManager.get().runWithLockForTable(runnable, ACQUIRE_TIMEOUT, table);
    }
}
