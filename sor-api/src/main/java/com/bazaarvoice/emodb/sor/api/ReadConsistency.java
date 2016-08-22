package com.bazaarvoice.emodb.sor.api;

public enum ReadConsistency {

    /**
     * Read at maximum performance, potentially returning stale data.
     */
    WEAK,

    /**
     * Read such that all previous strong consistency writes in the same data center are
     * guaranteed to be included in the result.
     */
    STRONG,
}
