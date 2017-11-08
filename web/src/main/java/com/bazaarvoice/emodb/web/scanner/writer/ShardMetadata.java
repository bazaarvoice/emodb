package com.bazaarvoice.emodb.web.scanner.writer;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

import java.util.Map;

/**
 * POJO for metadata about a shard being written.
 */
public class ShardMetadata implements Comparable<ShardMetadata> {
    private final String _tableName;
    private final String _placement;
    private final Map<String, Object> _tableMetadata;
    private final int _shardId;
    private final long _tableUuid;

    public ShardMetadata(String tableName, String placement, Map<String, Object> tableMetadata, int shardId, long tableUuid) {
        _tableName = tableName;
        _placement = placement;
        _tableMetadata = tableMetadata;
        _shardId = shardId;
        _tableUuid = tableUuid;
    }

    public String getTableName() {
        return _tableName;
    }

    public String getPlacement() {
        return _placement;
    }

    public Map<String, Object> getTableMetadata() {
        return _tableMetadata;
    }

    public int getShardId() {
        return _shardId;
    }

    public long getTableUuid() {
        return _tableUuid;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ShardMetadata)) {
            return false;
        }

        ShardMetadata that = (ShardMetadata) o;

        return _shardId == that._shardId &&
                _tableUuid == that._tableUuid &&
                Objects.equal(_tableName, that._tableName) &&
                Objects.equal(_placement, that._placement) &&
                Objects.equal(_tableMetadata, that._tableMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_shardId, _tableUuid);
    }

    @Override
    public int compareTo(ShardMetadata o) {
        return ComparisonChain.start()
                .compare(_shardId, o._shardId)
                .compare(_tableUuid, o._tableUuid)
                .result();
    }

    public String toString() {
        return String.format("%02x_%016x", _shardId, _tableUuid);
    }
}
