package com.bazaarvoice.emodb.blob.api;

import java.util.Objects;

public final class Range {
    private final long _offset;
    private final long _length;

    public static Range satisfiableRange(long offset, long length) {
        if (length <= 0) {
            throw new RangeNotSatisfiableException(null, offset, length);
        }
        return new Range(offset, length);
    }

    public Range(long offset, long length) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must be >=0");
        }
        if (length < 0) {
            throw new IllegalArgumentException("Length must be >=0");
        }
        _offset = offset;
        _length = length;
    }

    public long getOffset() {
        return _offset;
    }

    public long getLength() {
        return _length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Range)) {
            return false;
        }
        Range range = (Range) o;
        return _length == range._length && _offset == range._offset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(_offset, _length);
    }

    @Override
    public String toString() {
        return _offset + ":" + _length;
    }
}