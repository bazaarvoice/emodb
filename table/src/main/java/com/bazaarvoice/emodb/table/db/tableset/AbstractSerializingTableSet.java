package com.bazaarvoice.emodb.table.db.tableset;

import static java.util.Objects.requireNonNull;

/**
 * Base class for TableSets which use a TableSerializer to load tables and convert them to an intermediate format
 * for caching and reuse.
 */
abstract public class AbstractSerializingTableSet implements SerializingTableSet {

    private final TableSerializer _tableSerializer;

    protected AbstractSerializingTableSet(TableSerializer tableSerializer) {
        _tableSerializer = requireNonNull(tableSerializer, "tableSerializer");
    }

    @Override
    public TableSerializer getTableSerializer() {
        return _tableSerializer;
    }
}
