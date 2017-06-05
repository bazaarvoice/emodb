package com.bazaarvoice.emodb.sor.db.astyanax;

import com.datastax.driver.core.TableMetadata;

/**
 * Metadata about a Cassandra table.  Not called "TableMetadata" so as not to be confused with the Datastax object
 * of the same name.
 */
public class DeltaTableDDL extends TableDDL {
    private final String _blockColumnName;

    public DeltaTableDDL(TableMetadata tableMetadata, String rowKeyColumnName, String changeIdColumnName, String valueColumnName, String blockColumnName) {
        super(tableMetadata, rowKeyColumnName, changeIdColumnName, valueColumnName);
        _blockColumnName = blockColumnName;
    }

    public String getBlockColumnName() {
        return _blockColumnName;
    }
}
