package com.bazaarvoice.emodb.table.db.tableset;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Base class for TableSets which use a TableSerializer to load tables and convert them to an intermediate format
 * for caching and reuse.
 */
abstract public class AbstractSerializingTableSet implements SerializingTableSet {

    private final TableSerializer _tableSerializer;

    protected AbstractSerializingTableSet(TableSerializer tableSerializer) {
        _tableSerializer = checkNotNull(tableSerializer, "tableSerializer");
    }

    @Override
    public TableSerializer getTableSerializer() {
        return _tableSerializer;
    }
}
