package com.bazaarvoice.emodb.blob.api;

public interface RangeSpecification {
    /**
     * Converts a range specification into a specific range, given the actual size of a blob.
     * @throws RangeNotSatisfiableException if the computed range does not contain at least one byte.
     */
    Range getRange(long blobLength) throws RangeNotSatisfiableException;
}
