package com.bazaarvoice.emodb.sor.db.astyanax;

import com.datastax.driver.core.TableMetadata;

/**
 * Metadata about a Cassandra table.  Not called "TableMetadata" so as not to be confused with the Datastax object
 * of the same name.
 */
public class TableDDL {
    private final TableMetadata _tableMetadata;
    private final String _rowKeyColumnName;
    private final String _changeIdColumnName;
    private final String _valueColumnName;

    public TableDDL(TableMetadata tableMetadata, String rowKeyColumnName, String changeIdColumnName, String valueColumnName) {
        _tableMetadata = tableMetadata;
        _rowKeyColumnName = rowKeyColumnName;
        _changeIdColumnName = changeIdColumnName;
        _valueColumnName = valueColumnName;
    }

    public TableMetadata getTableMetadata() {
        return _tableMetadata;
    }

    public String getRowKeyColumnName() {
        return _rowKeyColumnName;
    }

    public String getChangeIdColumnName() {
        return _changeIdColumnName;
    }

    public String getValueColumnName() {
        return _valueColumnName;
    }
}
