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
    private final ColumnFamily<ByteBuffer, DeltaKey> _blockedDeltaColumnFamily;
    private final ColumnFamily<ByteBuffer, UUID> _deltaHistoryColumnFamily;
    private final BlockedDeltaTableDDL _blockedDeltaTableDDL;
    private final TableDDL _deltaHistoryTableDDL;

    DeltaPlacement(String name,
                   CassandraKeyspace keyspace,
                   ColumnFamily<ByteBuffer, DeltaKey> blockedDeltaColumnFamily,
                   ColumnFamily<ByteBuffer, UUID> deltaHistoryColumnFamily) {
        _name = checkNotNull(name, "name");
        _keyspace = checkNotNull(keyspace, "keyspace");
        _blockedDeltaColumnFamily = checkNotNull(blockedDeltaColumnFamily, "blockedDeltaColumnFamily");
        _deltaHistoryColumnFamily = checkNotNull(deltaHistoryColumnFamily, "deltaHistoryColumnFamily");

        _blockedDeltaTableDDL = createBlockedDeltaTableDDL(blockedDeltaColumnFamily.getName());
        _deltaHistoryTableDDL = creatHistoryTableDDL(_deltaHistoryColumnFamily.getName());
    }

    private TableDDL creatHistoryTableDDL(String tableName) {
        TableMetadata tableMetadata = _keyspace.getKeyspaceMetadata().getTable(tableName);
        String rowKeyColumnName = tableMetadata.getPrimaryKey().get(0).getName();
        String timeSeriesColumnName = tableMetadata.getPrimaryKey().get(1).getName();
        String valueColumnName = tableMetadata.getColumns().get(2).getName();

        return new TableDDL(tableMetadata, rowKeyColumnName, timeSeriesColumnName, valueColumnName);
    }

    private BlockedDeltaTableDDL createBlockedDeltaTableDDL(String tableName) {
        TableMetadata tableMetadata = _keyspace.getKeyspaceMetadata().getTable(tableName);
        String rowKeyColumnName = tableMetadata.getPrimaryKey().get(0).getName();
        String timeSeriesColumnName = tableMetadata.getPrimaryKey().get(1).getName();
        String blockColumnName = tableMetadata.getPrimaryKey().get(2).getName();
        String valueColumnName = tableMetadata.getColumns().get(3).getName();

        return new BlockedDeltaTableDDL(tableMetadata, rowKeyColumnName, timeSeriesColumnName, valueColumnName, blockColumnName);
    }

    @Override
    public String getName() {
        return _name;
    }

    @Override
    public CassandraKeyspace getKeyspace() {
        return _keyspace;
    }

    ColumnFamily<ByteBuffer, UUID> getDeltaHistoryColumnFamily() {
        return _deltaHistoryColumnFamily;
    }

    ColumnFamily<ByteBuffer, DeltaKey> getBlockedDeltaColumnFamily() {
        return _blockedDeltaColumnFamily;
    }

    TableDDL getDeltaHistoryTableDDL() {
        return _deltaHistoryTableDDL;
    }

    BlockedDeltaTableDDL getBlockedDeltaTableDDL() {
        return _blockedDeltaTableDDL;
    }

    // for debugging
    @Override
    public String toString() {
        return _name;
    }
}
