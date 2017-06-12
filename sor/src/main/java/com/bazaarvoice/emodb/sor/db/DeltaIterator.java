package com.bazaarvoice.emodb.sor.db;


import com.datastax.driver.core.Row;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public class DeltaIterator extends AbstractIterator<Row> {

    private static final int VALUE_RESULT_SET_COLUMN = 2;
    private static final int BLOCK_RESULT_SET_COLUMN = 3;
    List<Row> list;

    private Iterator<Row> _iterator;
    Row _next;

    public DeltaIterator(Iterator<Row> iterator) {
        _iterator = iterator;
        if (iterator.hasNext()) {
            _next = iterator.next();
        }
    }

    @Override
    protected Row computeNext() {
        if (_next == null) {
            return endOfData();
        }
        if (!_iterator.hasNext()) {
            Row ret = _next;
            _next = null;
            return ret;
        }
        Row upcoming = _iterator.next();
        if (getBlock(upcoming) == 0) {
            Row ret = _next;
            _next = upcoming;
            return ret;
        }

        if (list == null) {
            list = Lists.newArrayListWithCapacity(3);
        }
        int contentSize = getValue(_next).remaining() + getValue(upcoming).remaining();
        list.add(_next);
        list.add(upcoming);

        while (_iterator.hasNext() && getBlock(_next = _iterator.next()) != 0) {
            list.add(_next);
            contentSize += getValue(_next).remaining();
            _next = null;
        }
        // construct new row object from list
        Row ret = new StitchedRow(list, VALUE_RESULT_SET_COLUMN, contentSize);

        list.clear();

        return ret;

    }

    private int getBlock(Row row) {
        return row.getInt(BLOCK_RESULT_SET_COLUMN);
    }

    private ByteBuffer getValue(Row row) {
        return row.getBytesUnsafe(VALUE_RESULT_SET_COLUMN);
    }
}

