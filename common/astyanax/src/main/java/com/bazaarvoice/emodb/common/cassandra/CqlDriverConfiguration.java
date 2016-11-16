package com.bazaarvoice.emodb.common.cassandra;

/**
 * Configuration settings for the CQL driver
 */
public class CqlDriverConfiguration {

    private static final int DEFAULT_MAX_RANDOM_ROWS_BATCH_SIZE = 50;
    private static final int DEFAULT_SINGLE_ROW_FETCH_SIZE = 100;
    private static final int DEFAULT_SINGLE_ROW_PREFETCH_LIMIT = 50;
    private static final int DEFAULT_MULTI_ROW_FETCH_SIZE = 100;
    private static final int DEFAULT_MULTI_ROW_PREFETCH_LIMIT = 50;
    private static final int DEFAULT_RECORD_CACHE_SIZE = 20;
    private static final int DEFAULT_RECORD_SOFT_CACHE_SIZE = 10;

    /**
     * Fetch sizes determine the number of rows the CQL driver will stream into memory for a given query.  For example,
     * if a query contains 1,000 matching rows and the fetch size is 100 then it will be streamed from Cassandra in
     * batches of 100 rows.  This helps limit the memory requirements for queries with extremely large result sets.
     */
    private int _singleRowFetchSize = DEFAULT_SINGLE_ROW_FETCH_SIZE;
    private int _multiRowFetchSize = DEFAULT_MULTI_ROW_FETCH_SIZE;

    /**
     * Prefetch limits determine the point at which more results are asynchronously fetched for an open query.  For
     * example, assume a query has a fetch size of 500.  After fetching and iterating over the first 500 rows the CQL
     * driver synchronously fetches the next 500 rows in the result set.  This results in a synchronous delay iterating
     * to the 501st row.  With a prefetch limit of 200, for example, the next 500 rows are asynchronously fetched once
     * the first 300 rows have been iterated from the last fetch and there are 200 rows remaining in memory.  This
     * decreases or possibly eliminates the fetch delay at the cost of requiring more room in memory for result rows.
     */
    private int _singleRowPrefetchLimit = DEFAULT_SINGLE_ROW_PREFETCH_LIMIT;
    private int _multiRowPrefetchLimit = DEFAULT_MULTI_ROW_PREFETCH_LIMIT;

    /**
     * Maximum number of rows to fetch in a single batch when querying for multiple keys.
     */
    private int _maxRandomRowsBatchSize = DEFAULT_MAX_RANDOM_ROWS_BATCH_SIZE;

    /**
     * Cache sizes for each record's columns in memory.  Larger values decrease the chance of needing to re-fetch
     * rows while increasing the data reader's memory utilization.
     */
    private int _recordCacheSize = DEFAULT_RECORD_CACHE_SIZE;
    private int _recordSoftCacheSize = DEFAULT_RECORD_SOFT_CACHE_SIZE;

    public int getMaxRandomRowsBatchSize() {
        return _maxRandomRowsBatchSize;
    }

    public void setMaxRandomRowsBatchSize(int maxRandomRowsBatchSize) {
        _maxRandomRowsBatchSize = maxRandomRowsBatchSize;
    }

    public int getSingleRowFetchSize() {
        return _singleRowFetchSize;
    }

    public void setSingleRowFetchSize(int singleRowFetchSize) {
        _singleRowFetchSize = singleRowFetchSize;
    }

    public int getSingleRowPrefetchLimit() {
        return _singleRowPrefetchLimit;
    }

    public void setSingleRowPrefetchLimit(int singleRowPrefetchLimit) {
        _singleRowPrefetchLimit = singleRowPrefetchLimit;
    }

    public int getMultiRowFetchSize() {
        return _multiRowFetchSize;
    }

    public void setMultiRowFetchSize(int multiRowFetchSize) {
        _multiRowFetchSize = multiRowFetchSize;
    }

    public int getMultiRowPrefetchLimit() {
        return _multiRowPrefetchLimit;
    }

    public void setMultiRowPrefetchLimit(int multiRowPrefetchLimit) {
        _multiRowPrefetchLimit = multiRowPrefetchLimit;
    }

    public int getRecordCacheSize() {
        return _recordCacheSize;
    }

    public void setRecordCacheSize(int recordCacheSize) {
        _recordCacheSize = recordCacheSize;
    }

    public int getRecordSoftCacheSize() {
        return _recordSoftCacheSize;
    }

    public void setRecordSoftCacheSize(int recordSoftCacheSize) {
        _recordSoftCacheSize = recordSoftCacheSize;
    }
}