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
    private final ColumnFamily<ByteBuffer, DeltaKey> _blockedDeltaColumnFamily;
    private final ColumnFamily<ByteBuffer, UUID> _deltaHistoryColumnFamily;
    private final TableDDL _deltaTableDDL;
    private final BlockedDeltaTableDDL _blockedDeltaTableDDL;
    private final TableDDL _deltaHistoryTableDDL;

    DeltaPlacement(String name,
                   CassandraKeyspace keyspace,
                   ColumnFamily<ByteBuffer, UUID> deltaColumnFamily,
                   ColumnFamily<ByteBuffer, DeltaKey> blockedDeltaColumnFamily,
                   ColumnFamily<ByteBuffer, UUID> deltaHistoryColumnFamily) {
        _name = checkNotNull(name, "name");
        _keyspace = checkNotNull(keyspace, "keyspace");
        _deltaColumnFamily = checkNotNull(deltaColumnFamily, "deltaColumnFamily");
        _blockedDeltaColumnFamily = checkNotNull(blockedDeltaColumnFamily, "blockedDeltaColumnFamily");
        _deltaHistoryColumnFamily = checkNotNull(deltaHistoryColumnFamily, "deltaHistoryColumnFamily");

        _deltaTableDDL = createTableDDL(_deltaColumnFamily.getName());
        _blockedDeltaTableDDL = createBlockedDeltaTableDDL(blockedDeltaColumnFamily.getName());
        _deltaHistoryTableDDL = createTableDDL(_deltaHistoryColumnFamily.getName());
    }

    /**
     * Both placement tables -- delta, and delta history -- follow the same DDL.
     */
    private TableDDL createTableDDL(String tableName) {
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

    ColumnFamily<ByteBuffer, UUID> getDeltaColumnFamily() {
        return _deltaColumnFamily;
    }

    ColumnFamily<ByteBuffer, UUID> getDeltaHistoryColumnFamily() {
        return _deltaHistoryColumnFamily;
    }

    ColumnFamily<ByteBuffer, DeltaKey> getBlockedDeltaColumnFamily() {
        return _blockedDeltaColumnFamily;
    }

    TableDDL getDeltaTableDDL() {
        return _deltaTableDDL;
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
