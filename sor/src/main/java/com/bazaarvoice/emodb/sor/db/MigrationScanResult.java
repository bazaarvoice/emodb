package com.bazaarvoice.emodb.sor.db;

import java.nio.ByteBuffer;
import java.util.UUID;

public class MigrationScanResult {
    private final ByteBuffer _rowKey;
    private final UUID _changeId;
    private final ByteBuffer _value;

    public MigrationScanResult(ByteBuffer rowKey, UUID changeId, ByteBuffer value) {
        _rowKey = rowKey;
        _changeId = changeId;
        _value = value;
    }

    public ByteBuffer getRowKey() {
        return _rowKey;
    }

    public UUID getChangeId() {
        return _changeId;
    }

    public ByteBuffer getValue() {
        return _value;
    }
}
