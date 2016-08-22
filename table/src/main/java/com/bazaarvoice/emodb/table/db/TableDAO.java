package com.bazaarvoice.emodb.table.db;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.FacadeExistsException;
import com.bazaarvoice.emodb.sor.api.FacadeOptions;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnknownFacadeException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.google.common.base.Optional;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public interface TableDAO {

    void create(String name, TableOptions options, Map<String, ?> attributes, Audit audit)
            throws TableExistsException;

    void createFacade(String name, FacadeOptions options, Audit audit)
            throws FacadeExistsException;

    /**
     * This throws an exception if a facade is not allowed.
     * Returns false if adding a facade would result in an idempotent operation.
     * Return true if facade is allowed
     */
    boolean checkFacadeAllowed(String name, FacadeOptions options) throws TableExistsException;

    void drop(String name, Audit audit) throws UnknownTableException;

    void dropFacade(String name, String placement, Audit audit) throws UnknownFacadeException;

    void move(String name, String destPlacement, Optional<Integer> numShards, Audit audit, MoveType moveType) throws UnknownTableException;

    void moveFacade(String name, String sourcePlacement, String destPlacement, Optional<Integer> numShards, Audit audit, MoveType moveType) throws UnknownFacadeException;

    void setAttributes(String name, Map<String, ?> attributes, Audit audit) throws UnknownTableException;

    void audit(String name, String op, Audit audit);

    Iterator<Table> list(@Nullable String fromNameExclusive, LimitCounter limit);

    boolean exists(String name);

    boolean isMoveToThisPlacementAllowed(String placement);

    /**
     * Returns the correct table/facade for the current data center
     */
    Table get(String name) throws UnknownTableException;

    Table getByUuid(long uuid) throws UnknownTableException, DroppedTableException;

    Collection<String> getTablePlacements(boolean includeInternal, boolean localOnly);

    TableSet createTableSet();
}
