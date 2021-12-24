package com.bazaarvoice.emodb.blob.api;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.hash;

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
        checkArgument(offset >= 0, "Offset must be >=0");
        checkArgument(length >= 0, "Length must be >=0");
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
        return hash(_offset, _length);
    }

    @Override
    public String toString() {
        return _offset + ":" + _length;
    }
}