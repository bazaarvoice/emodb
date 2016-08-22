package com.bazaarvoice.emodb.web.scanner.writer;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

/**
 * POJO key for identifying a unique ScanWriter file transfer.
 */
public class TransferKey implements Comparable<TransferKey> {
    private final long _tableUuid;
    private final int _shardId;

    public TransferKey(long tableUuid, int shardId) {
        _tableUuid = tableUuid;
        _shardId = shardId;
    }

    public long getTableUuid() {
        return _tableUuid;
    }

    public int getShardId() {
        return _shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TransferKey)) {
            return false;
        }

        TransferKey that = (TransferKey) o;

        return _tableUuid == that._tableUuid &&
                _shardId == that._shardId;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_tableUuid, _shardId);
    }

    @Override
    public int compareTo(TransferKey o) {
        return ComparisonChain.start()
                .compare(_shardId, o._shardId)
                .compare(_tableUuid, o._tableUuid)
                .result();
    }

    public String toString() {
        return String.format("%02x_%016x", _shardId, _tableUuid);
    }
}
