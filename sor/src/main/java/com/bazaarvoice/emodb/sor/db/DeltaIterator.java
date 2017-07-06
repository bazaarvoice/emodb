package com.bazaarvoice.emodb.sor.db;


import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import com.netflix.astyanax.serializers.StringSerializer;

import javax.inject.Named;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


/**
 *  Iterator abstraction that stitches blocked deltas together
 */
abstract public class DeltaIterator<R, T> extends AbstractIterator<T> {

    private List<R> _list;

    private final Iterator<R> _iterator;
    private R _next;
    private final boolean _reverse;
    private final int _prefixLength;

    public DeltaIterator(Iterator<R> iterator, boolean reverse, int prefixLength) {
        _iterator = iterator;
        if (iterator.hasNext()) {
            _next = iterator.next();
        }
        _reverse = reverse;
        _prefixLength = prefixLength;
    }

    // stitch delta together in reverse
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

    // stitch delta together and return it as one bytebuffer
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
        // Last item in iterator, no need to attempt stitching
        if (!_iterator.hasNext()) {
            T ret = convertDelta(_next);
            _next = null;
            return ret;
        }
        R upcoming = _iterator.next();
        ByteBuffer content;

        if (!_reverse) {
            // no need to stitch if true, as the next row is the beginning a new delta
            if (getBlock(upcoming) == 0) {
                T ret = convertDelta(_next);
                _next = upcoming;
                return ret;
            }
            content = compute(upcoming);
        }
        else {
            // no need to stitch if true, as we the current row is the beginning of the delta
            if (getBlock(_next) == 0) {
                T ret = convertDelta(_next);
                _next = upcoming;
                return ret;
            }
            content = reverseCompute(upcoming);
        }

        return convertDelta(upcoming, content);

    }

    // This handles the edge case in which a client has explicity specified a changeId when writing and overwrote an exisiting delta.
    private void skipForward() {
        _next = null;
        while (_iterator.hasNext() && getBlock(_next = _iterator.next()) != 0) {
            _next = null;
        }
    }

    // converts utf-8 encoded hex to int by building it digit by digit.
    private int getNumBlocks(R delta) {
        ByteBuffer content = getValue(delta);
        int numBlocks = 0;

        // build numBlocks by adding together each hex digit
        for (int i = 0; i < _prefixLength; i++) {
            byte b = content.get();
            numBlocks += (b <= '9' ? b - '0' : b - 'A' + 10) << (4 * (_prefixLength - i - 1));
        }

        return numBlocks;
    }

    private ByteBuffer stitchContent(int contentSize) {
        ByteBuffer content = ByteBuffer.allocate(contentSize);
        for (R delta : _list) {
            content.put(getValue(delta));
        }
        content.position(0);
        _list.clear();
        return content;
    }

    abstract protected T convertDelta(R delta);

    abstract protected T convertDelta(R delta, ByteBuffer content);

    abstract protected int getBlock(R delta);

    abstract protected ByteBuffer getValue(R delta);

}

