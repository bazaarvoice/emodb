package com.bazaarvoice.emodb.sor.db;


import com.datastax.driver.core.Row;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

abstract public class DeltaIterator<R, T> extends AbstractIterator<T> {

    private List<R> _list;

    private Iterator<R> _iterator;
    private R _next;

    public DeltaIterator(Iterator<R> iterator) {
        _iterator = iterator;
        if (iterator.hasNext()) {
            _next = iterator.next();
        }
    }

    @Override
    protected T computeNext() {
        if (_next == null) {
            return endOfData();
        }
        if (!_iterator.hasNext()) {
            T ret = convertDelta(_next);
            _next = null;
            return ret;
        }
        R upcoming = _iterator.next();
        if (getBlock(upcoming) == 0) {
            T ret = convertDelta(_next);
            _next = upcoming;
            return ret;
        }

        if (_list == null) {
            _list = Lists.newArrayListWithCapacity(3);
        }
        int contentSize = getValue(_next).remaining() + getValue(upcoming).remaining();
        _list.add(_next);
        _list.add(upcoming);

        while (_iterator.hasNext() && getBlock(_next = _iterator.next()) != 0) {
            _list.add(_next);
            contentSize += getValue(_next).remaining();
            _next = null;
        }
        // construct new row object from list

        ByteBuffer content = ByteBuffer.allocate(contentSize);
        int position = content.position();
        for (R delta : _list) {
            content.put(getValue(delta));
        }
        content.position(position);

        _list.clear();

        return convertDelta(upcoming, content);

    }

    abstract protected T convertDelta(R delta);

    abstract protected T convertDelta(R delta, ByteBuffer content);

    abstract protected int getBlock(R delta);

    abstract protected ByteBuffer getValue(R delta);

//    private int getBlock(Row row) {
//        return row.getInt(BLOCK_RESULT_SET_COLUMN);
//    }
//
//    private ByteBuffer getValue(Row row) {
//        return row.getBytesUnsafe(VALUE_RESULT_SET_COLUMN);
//    }
}

