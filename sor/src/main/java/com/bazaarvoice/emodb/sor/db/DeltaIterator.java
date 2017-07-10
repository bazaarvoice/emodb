package com.bazaarvoice.emodb.sor.db;


import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;


/**
 *  Iterator abstraction that stitches blocked deltas together
 */
abstract public class DeltaIterator<R, T> extends AbstractIterator<T> {

    private List<R> _list;

    private final Iterator<R> _iterator;
    private R _next;
    private R _oldDelta;
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
    private ByteBuffer reverseCompute() {
        int contentSize = getValue(_next).remaining();
        UUID correctChangeId = getChangeId(_next);

        if (_list == null) {
            _list = Lists.newArrayListWithCapacity(3);
        }

        _list.add(_next);


        _oldDelta = _next;

        while (_iterator.hasNext() && getChangeId(_next = _iterator.next()) == correctChangeId) {
            _list.add(_next);
            contentSize += getValue(_next).remaining();
            _next = null;
        }

        Collections.reverse(_list);


        if (getBlock(_list.get(0)) != 0) {
            return null;
        }

        int numBlocks = getNumBlocks(_list.get(0));

        while (numBlocks != _list.size()) {
            contentSize -= getValue(_list.remove(_list.size())).remaining();
        }

        return stitchContent(contentSize);

    }

    // stitch delta together and return it as one bytebuffer
    private ByteBuffer compute(int numBlocks) {

        int contentSize = getValue(_next).remaining();
        _oldDelta = _next;
        UUID changeId = getChangeId(_next);

        if (_list == null) {
            _list = Lists.newArrayListWithCapacity(numBlocks);
        }

        _list.add(_next);

        for (int i = 1; i < numBlocks; i++) {
            if (_iterator.hasNext()) {
                _next = _iterator.next();
                if (getChangeId(_next) == changeId) {
                    _list.add(_next);
                    contentSize += getValue(_next).remaining();
                } else {
                    // fragmented delta encountered, we must skip over it
                    skipForward();
                    return null;
                }
            } else {
                // fragmented delta and no other deltas to skip to
                throw new DeltaStitchingException();
            }
        }

        skipForward();

        return stitchContent(contentSize);
    }

    @Override
    protected T computeNext() {
        if (_next == null) {
            return endOfData();
        }

        ByteBuffer content;

        if (!_reverse) {

            int numBlocks = getNumBlocks(_next);

            if (numBlocks == 1) {
                T ret = convertDelta(_next);
                skipForward();
                return ret;
            }

            content = compute(numBlocks);

        } else {
            if (getBlock(_next) == 0) {
                T ret = convertDelta(_next);
                _next = _iterator.next();
                return ret;
            }

            content = reverseCompute();
        }

        _list.clear();

        if (content == null) {
            return computeNext();
        }


        return convertDelta(_oldDelta, content);
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
        return content;
    }

    abstract protected T convertDelta(R delta);

    abstract protected T convertDelta(R delta, ByteBuffer content);

    abstract protected int getBlock(R delta);

    abstract protected UUID getChangeId(R delta);

    abstract protected ByteBuffer getValue(R delta);

}

class DeltaStitchingException extends RuntimeException {

    public DeltaStitchingException() {
        super("Found fragmented deltas without a compaction record ahead of them.");
    }
}

