package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.table.db.astyanax.Placement;
import com.datastax.driver.core.TableMetadata;
import com.netflix.astyanax.model.ColumnFamily;

import java.nio.ByteBuffer;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A Cassandra keyspace and the DataStore-related column families.
 */
class DeltaPlacement implements Placement {
    private final String _name;
    private final CassandraKeyspace _keyspace;
    private final ColumnFamily<ByteBuffer, UUID> _deltaColumnFamily;
    private final ColumnFamily<ByteBuffer, UUID> _auditColumnFamily;
    private final ColumnFamily<ByteBuffer, UUID> _deltaHistoryColumnFamily;
    private final TableDDL _deltaTableDDL;
    private final TableDDL _auditTableDDL;
    private final TableDDL _deltaHistoryTableDDL;

    DeltaPlacement(String name,
                   CassandraKeyspace keyspace,
                   ColumnFamily<ByteBuffer, UUID> deltaColumnFamily,
                   ColumnFamily<ByteBuffer, UUID> auditColumnFamily,
                   ColumnFamily<ByteBuffer, UUID> deltaHistoryColumnFamily) {
        _name = checkNotNull(name, "name");
        _keyspace = checkNotNull(keyspace, "keyspace");
        _deltaColumnFamily = checkNotNull(deltaColumnFamily, "deltaColumnFamily");
        _auditColumnFamily = checkNotNull(auditColumnFamily, "auditColumnFamily");
        _deltaHistoryColumnFamily = checkNotNull(deltaHistoryColumnFamily, "deltaHistoryColumnFamily");

        _deltaTableDDL = createTableDDL(_deltaColumnFamily.getName());
        _auditTableDDL = createTableDDL(_auditColumnFamily.getName());
        _deltaHistoryTableDDL = createTableDDL(_deltaHistoryColumnFamily.getName());
    }

    /**
     * All three placement tables -- delta, audit, and delta history -- follow the same DDL.
     */
    private TableDDL createTableDDL(String tableName) {
        TableMetadata tableMetadata = _keyspace.getKeyspaceMetadata().getTable(tableName);
        String rowKeyColumnName = tableMetadata.getPrimaryKey().get(0).getName();
        String timeSeriesColumnName = tableMetadata.getPrimaryKey().get(1).getName();
        String valueColumnName = tableMetadata.getColumns().get(2).getName();

        return new TableDDL(tableMetadata, rowKeyColumnName, timeSeriesColumnName, valueColumnName);
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public CassandraKeyspace getKeyspace() {
        return _keyspace;
    }

    ColumnFamily<ByteBuffer, UUID> getDeltaColumnFamily() {
        return _deltaColumnFamily;
    }

    ColumnFamily<ByteBuffer, UUID> getAuditColumnFamily() {
        return _auditColumnFamily;
    }

    ColumnFamily<ByteBuffer, UUID> getDeltaHistoryColumnFamily() {
        return _deltaHistoryColumnFamily;
    }

    TableDDL getDeltaTableDDL() {
        return _deltaTableDDL;
    }

    TableDDL getAuditTableDDL() {
        return _auditTableDDL;
    }

    TableDDL getDeltaHistoryTableDDL() {
        return _deltaHistoryTableDDL;
    }

    // for debugging
    @Override
    public String toString() {
        return _name;
    }
}
