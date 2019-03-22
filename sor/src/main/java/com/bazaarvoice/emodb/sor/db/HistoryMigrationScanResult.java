package com.bazaarvoice.emodb.sor.db;

import java.nio.ByteBuffer;
import java.util.UUID;

public class HistoryMigrationScanResult extends MigrationScanResult {
    private final int _ttl;

    public HistoryMigrationScanResult(ByteBuffer rowKey, UUID changeId, ByteBuffer value, int ttl) {
        super(rowKey, changeId, value);
        _ttl = ttl;
    }

    public int getTtl() {
        return _ttl;
    }
}
