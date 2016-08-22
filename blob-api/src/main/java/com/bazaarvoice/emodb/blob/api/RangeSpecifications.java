package com.bazaarvoice.emodb.blob.api;

public abstract class RangeSpecifications {

    /** Prevent instantiation. */
    private RangeSpecifications() {}

    /**
     * Returns all bytes from position {@code offset} (inclusive) to the end of the blob.
     * If {@code offset} is out of range (larger than the blob length) the range is unsatisfiable.
     */
    public static RangeSpecification slice(long offset) {
        return new OffsetRangeSpecification(offset, null);
    }

    /**
     * Returns up to {@code length} bytes starting from {@code offset} (inclusive).
     * If {@code offset} is out of range (larger than the blob length) then the range is unsatisfiable.  If
     * {@code offset + length} is larger than the blob length then all bytes starting from {@code offset} will be
     * included in the range.
     */
    public static RangeSpecification slice(long offset, long length) {
        return new OffsetRangeSpecification(offset, length);
    }

    /**
     * Returns the last {@code length} bytes of the blob.  If the blob is smaller than {@code length}, all bytes in
     * the blob will be returned unless the blob has a length of zero, in which case the range is unsatisfiable.
     */
    public static RangeSpecification suffix(long length) {
        return new SuffixRangeSpecification(length);
    }
}
