package com.bazaarvoice.emodb.table.db.generic;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.api.*;
import com.bazaarvoice.emodb.table.db.*;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.astyanax.MaintenanceChecker;
import com.bazaarvoice.emodb.table.db.astyanax.MaintenanceDAO;
import com.google.common.base.Optional;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class LockingTableDAO implements TableDAO {

    private final TableDAO _delegate;
    private final MaintenanceChecker _maintenanceChecker;

    @Inject
    public LockingTableDAO(@LockingTableDAODelegate TableDAO delegate, MaintenanceChecker maintenanceChecker) {
        _delegate = requireNonNull(delegate);
        _maintenanceChecker = requireNonNull(maintenanceChecker);
    }

    private void performActionIfNoMaintenance(String table, Runnable action) {
        if (!_maintenanceChecker.isTableUnderMaintenance(table)) {
            action.run();
        } else {
            throw new IllegalArgumentException(String.format("This table name is currently undergoing maintenance and therefore cannot be modified: %s", table));
        }
    }

    @Override
    public void create(String name, TableOptions options, Map<String, ?> attributes, Audit audit) throws TableExistsException {
        performActionIfNoMaintenance(name, () -> _delegate.create(name, options, attributes, audit));
    }

    @Override
    public void createFacade(String name, FacadeOptions options, Audit audit) throws FacadeExistsException {
        performActionIfNoMaintenance(name, () -> _delegate.createFacade(name, options, audit));
    }

    @Override
    public boolean checkFacadeAllowed(String name, FacadeOptions options) throws TableExistsException {
        return _delegate.checkFacadeAllowed(name, options);
    }

    @Override
    public void drop(String name, Audit audit) throws UnknownTableException {
        performActionIfNoMaintenance(name, () -> _delegate.drop(name, audit));
    }

    @Override
    public void dropFacade(String name, String placement, Audit audit) throws UnknownFacadeException {
        performActionIfNoMaintenance(name, () -> _delegate.dropFacade(name, placement, audit));
    }

    @Override
    public void move(String name, String destPlacement, Optional<Integer> numShards, Audit audit, MoveType moveType) throws UnknownTableException {
        performActionIfNoMaintenance(name, () -> _delegate.move(name, destPlacement, numShards, audit, moveType));
    }

    @Override
    public void moveFacade(String name, String sourcePlacement, String destPlacement, Optional<Integer> numShards, Audit audit, MoveType moveType) throws UnknownFacadeException {
        performActionIfNoMaintenance(name, () -> _delegate.moveFacade(name, sourcePlacement, destPlacement, numShards, audit, moveType));
    }

    @Override
    public void setAttributes(String name, Map<String, ?> attributes, Audit audit) throws UnknownTableException {
        performActionIfNoMaintenance(name, () -> _delegate.setAttributes(name, attributes, audit));
    }

    @Override
    public void audit(String name, String op, Audit audit) {
        _delegate.audit(name, op, audit);
    }

    @Override
    public Iterator<Table> list(@Nullable String fromNameExclusive, LimitCounter limit) {
        return _delegate.list(fromNameExclusive, limit);
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
    public Table get(String name) throws UnknownTableException {
        return _delegate.get(name);
    }

    @Override
    public Table getByUuid(long uuid) throws UnknownTableException, DroppedTableException {
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

    @Override
    public void writeUnpublishedDatabusEvent(String name, UnpublishedDatabusEventType attribute) {
        _delegate.writeUnpublishedDatabusEvent(name, attribute);
    }

    @Override
    public Iterator<UnpublishedDatabusEvent> listUnpublishedDatabusEvents(Date fromInclusive, Date toExclusive) {
        return _delegate.listUnpublishedDatabusEvents(fromInclusive, toExclusive);
    }
}
