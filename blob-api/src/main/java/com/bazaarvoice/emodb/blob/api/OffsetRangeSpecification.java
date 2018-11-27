package com.bazaarvoice.emodb.blob.api;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Range specification for a subset of a blob starting at a particular offset.
 */
class OffsetRangeSpecification implements RangeSpecification {
    private final long _offset;
    private final Long _length;

    public OffsetRangeSpecification(long offset, @Nullable Long length) {
        if (offset < 0) {
            throw new IllegalArgumentException("Range offset must be >=0");
        }
        if (length != null && length <= 0) {
            throw new IllegalArgumentException("Range length must be >0");
        }
        _offset = offset;
        _length = length;
    }

    @Override
    public Range getRange(long blobLength) throws RangeNotSatisfiableException {
        long length;
        if (_length != null) {
            length = Math.min(_length, blobLength - _offset);
        } else {
            length = blobLength - _offset;
        }
        return Range.satisfiableRange(_offset, length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OffsetRangeSpecification)) {
            return false;
        }
        OffsetRangeSpecification offsetRange = (OffsetRangeSpecification) o;
        return _offset == offsetRange._offset && Objects.equals(_length, offsetRange._length);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_offset, _length);
    }

    @Override
    public String toString() {
        // Format as an HTTP Range header
        StringBuilder buf = new StringBuilder();
        buf.append("bytes=");
        buf.append(_offset);
        buf.append('-');
        if (_length != null) {
            buf.append(_offset + _length - 1);  // End is inclusive
        }
        return buf.toString();
    }
}
