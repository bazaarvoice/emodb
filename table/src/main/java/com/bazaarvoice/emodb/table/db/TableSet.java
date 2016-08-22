package com.bazaarvoice.emodb.table.db;

import com.bazaarvoice.emodb.sor.api.UnknownTableException;

import java.io.Closeable;

/**
 * This class acts as a consistent view of the underlying tables in the system.  The contract for this interface
 * does not imply that all tables must be consistent as of a particular point in time.  Rather, the contract for this
 * interface is that after any table has been returned once all future calls returning the same table will see
 * an unchanged version of that table (same attributes, placement, and so on).  It is up to the implementation
 * whether the same instance will be returned or not.
 *
 * Presently only {@link #getByUuid(long)}} is implemented because there is no use case for other access patterns
 * (such as list() or getByName()), although they would be a natural extension to this interface if needed.
 */
public interface TableSet extends Closeable {

    Table getByUuid(long uuid) throws UnknownTableException, DroppedTableException;
}
