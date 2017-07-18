package com.bazaarvoice.emodb.sor.db;

import com.datastax.driver.core.Row;
import com.google.common.collect.AbstractIterator;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.UUID;

public class MigrationScanResult {
    private final Row _row;
    private final int _rowKeyIndex;
    private final int _changeIdIndex;
    private final int _contentIndex;

    public MigrationScanResult(Row row, int rowKeyIndex, int changeIdIndex, int contentIndex) {
        _row = row;
        _rowKeyIndex = rowKeyIndex;
        _changeIdIndex = changeIdIndex;
        _contentIndex = contentIndex;
    }

    public ByteBuffer getRowKey() {
        return _row.getBytesUnsafe(_rowKeyIndex);
    }

    public UUID getChangeId() {
        return _row.getUUID(_changeIdIndex);
    }

    public ByteBuffer getValue() {
        return _row.getBytesUnsafe(_contentIndex);
    }
}
