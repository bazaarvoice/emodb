package com.bazaarvoice.emodb.sor.db;


import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

abstract public class DeltaIterator<R, T> extends AbstractIterator<T> {

    private List<R> _list;

    private Iterator<R> _iterator;
    private R _next;
    private boolean _reverse;

    public DeltaIterator(Iterator<R> iterator, boolean reverse) {
        _iterator = iterator;
        if (iterator.hasNext()) {
            _next = iterator.next();
        }
        _reverse = reverse;
    }

    private ByteBuffer reverseCompute(R upcoming) {

        int contentSize = getValue(_next).remaining() + getValue(upcoming).remaining();

        if (_list == null) {
            _list = Lists.newArrayListWithCapacity(3);
        }

        _list.add(_next);
        _list.add(upcoming);
        _next = null;
        int previousBlock = getBlock(upcoming);

        while(_iterator.hasNext() && getBlock(_next = _iterator.next()) == --previousBlock) {
            _list.add(_next);
            contentSize += getValue((_next)).remaining();
            _next = null;
        }

        Collections.reverse(_list);

        int numBlocks = getNumBlocks(_list.get(0));
        while (numBlocks != _list.size()) {
            contentSize -= getValue(_list.remove(_list.size())).remaining();
        }

        return stitchContent(contentSize);

    }

    private ByteBuffer compute(R upcoming) {

        int numBlocks = getNumBlocks(_next);

        if (numBlocks == 1) {
            ByteBuffer ret = getValue(_next);
            skipForward();
            return ret;
        }

        if (_list == null) {
            _list = Lists.newArrayListWithCapacity(3);
        }

        int contentSize = getValue(_next).remaining() + getValue(upcoming).remaining();
        _list.add(_next);
        _list.add(upcoming);
        _next = null;

        for (int i = 2; i < numBlocks; i++) {
            _next = _iterator.next();
            _list.add(_next);
            contentSize += getValue(_next).remaining();
        }

        skipForward();

        return stitchContent(contentSize);
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
        ByteBuffer content;

        if (!_reverse) {
            if (getBlock(upcoming) == 0) {
                T ret = convertDelta(_next);
                _next = upcoming;
                return ret;
            }
            content = compute(upcoming);
        }
        else {
            if (getBlock(_next) == 0) {
                T ret = convertDelta(_next);
                _next = upcoming;
                return ret;
            }
            content = reverseCompute(upcoming);
        }

        return convertDelta(upcoming, content);

    }

    private void skipForward() {
        _next = null;
        while (_iterator.hasNext() && getBlock(_next = _iterator.next()) != 0) {
            _next = null;
        }
    }

    private int getNumBlocks(R delta) {
        ByteBuffer content = getValue(delta);
        int numBlocks = 0;
        int multiplier = 1;
        for (int i = 3; i >= 0; i--) {
            numBlocks += (content.get(content.position() + i)- 48) * multiplier;
            multiplier *= 10;
        }
        return numBlocks;
    }

    private ByteBuffer stitchContent(int contentSize) {
        ByteBuffer content = ByteBuffer.allocate(contentSize);
        int position = content.position();
        for (R delta : _list) {
            content.put(getValue(delta));
        }
        content.position(position);
        _list.clear();
        return content;
    }

    abstract protected T convertDelta(R delta);

    abstract protected T convertDelta(R delta, ByteBuffer content);

    abstract protected int getBlock(R delta);

    abstract protected ByteBuffer getValue(R delta);

}

