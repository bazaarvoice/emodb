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
    private final TableMetadata _deltaTableMetadata;
    private final String _key;
    private final String _deltaColName;
    private final String _deltaValueColName;

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
        _deltaTableMetadata = _keyspace.getKeyspaceMetadata().getTable(_deltaColumnFamily.getName());
        _key = _deltaTableMetadata.getPrimaryKey().get(0).getName();
        _deltaColName = _deltaTableMetadata.getPrimaryKey().get(1).getName();
        _deltaValueColName = _deltaTableMetadata.getColumns().get(2).getName();
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

    TableMetadata getDeltaTableMetadata() {
        return _deltaTableMetadata;
    }

    String getDeltaRowKeyColumnName() {
        return _key;
    }

    String getDeltaColumnName() {
        return _deltaColName;
    }

    String getDeltaValueColumnName() {
        return _deltaValueColName;
    }

    // for debugging
    @Override
    public String toString() {
        return _name;
    }
}
