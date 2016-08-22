package com.bazaarvoice.emodb.table.db.tableset;

import com.bazaarvoice.emodb.table.db.TableSet;

/**
 * Interface for TableSet implementations that use a TableSerializer to load and cache table metadata in an
 * intermediate byte format.
 */
public interface SerializingTableSet extends TableSet {
    TableSerializer getTableSerializer();
}
