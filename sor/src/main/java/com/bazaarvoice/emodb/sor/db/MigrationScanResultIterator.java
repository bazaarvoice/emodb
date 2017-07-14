package com.bazaarvoice.emodb.sor.db;

import com.datastax.driver.core.Row;
import com.google.common.collect.AbstractIterator;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.UUID;

public class MigrationScanResultIterator extends AbstractIterator<Row> {
    private final Iterator<Row> _iterator;
    private final int _rowKeyIndex;
    private final int _changeIdIndex;
    private final int _contentIndex;

    public MigrationScanResultIterator(Iterator<Row> iterator, int rowKeyIndex, int changeIdIndex, int contentIndex) {
        _iterator = iterator;
        _rowKeyIndex = rowKeyIndex;
        _changeIdIndex = changeIdIndex;
        _contentIndex = contentIndex;
    }

    public ByteBuffer getRowKey(Row row) {
        return row.getBytesUnsafe(_rowKeyIndex);
    }

    public UUID getChangeId(Row row) {
        return row.getUUID(_changeIdIndex);
    }

    public ByteBuffer getValue(Row row) {
        return row.getBytesUnsafe(_contentIndex);
    }

    @Override
    protected Row computeNext() {
        if (_iterator.hasNext()) {
            return _iterator.next();
        }

        return endOfData();
    }
}
