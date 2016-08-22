package com.bazaarvoice.emodb.sor.db;

import com.bazaarvoice.emodb.table.db.Table;

import java.nio.ByteBuffer;

public class MultiTableScanResult {
    private final ByteBuffer _rowKey;
    private final int _shardId;
    private final long _tableUuid;
    private final boolean _dropped;
    private final Record _record;

    public MultiTableScanResult(ByteBuffer rowKey, int shardId, long tableUuid, boolean dropped, Record record) {
        _rowKey = rowKey;
        _shardId = shardId;
        _tableUuid = tableUuid;
        _dropped = dropped;
        _record = record;
    }

    public ByteBuffer getRowKey() {
        return _rowKey;
    }

    public int getShardId() {
        return _shardId;
    }

    public long getTableUuid() {
        return _tableUuid;
    }

    public boolean isDropped() {
        return _dropped;
    }

    public Record getRecord() {
        return _record;
    }

    public Table getTable() {
        return _record.getKey().getTable();
    }
}
