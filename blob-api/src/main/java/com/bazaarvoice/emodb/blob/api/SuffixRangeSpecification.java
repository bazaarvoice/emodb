package com.bazaarvoice.emodb.blob.api;

/**
 * Range specification for the last {@code N} bytes in a blob.
 */
class SuffixRangeSpecification implements RangeSpecification {
    private final long _length;

    public SuffixRangeSpecification(long length) {
        // For some reason HTTP spec says a suffix len of 0 is valid.  But it's always unsatisfiable.
        if (length < 0) {
            throw new IllegalArgumentException("Suffix length must be >= 0");
        }
        _length = length;
    }

    @Override
    public Range getRange(long blobLength) throws RangeNotSatisfiableException {
        long length = Math.min(_length, blobLength);
        long offset = blobLength - length;
        return Range.satisfiableRange(offset, length);
    }

    @Override
    public boolean equals(Object o) {
        return (this == o) ||
                (o instanceof SuffixRangeSpecification && _length == ((SuffixRangeSpecification) o)._length);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(_length);
    }

    @Override
    public String toString() {
        // Format as an HTTP Range header
        return "bytes=-" + _length;
    }
}
