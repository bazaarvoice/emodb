package com.bazaarvoice.emodb.blob.db;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.table.db.Table;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;

/**
 * The interface Metadata provider.
 */
public interface MetadataProvider {

    /**
     * Scan metadata iterator.
     *
     * @param table               the table
     * @param fromBlobIdExclusive the from blob id exclusive
     * @param limit               the limit
     * @return the iterator
     */
    Iterator<Map.Entry<String, StorageSummary>> scanMetadata(Table table, @Nullable String fromBlobIdExclusive, LimitCounter limit);

    /**
     * Read metadata storage summary.
     *
     * @param table  the table
     * @param blobId the blob id
     * @return the storage summary
     */
    StorageSummary readMetadata(Table table, String blobId);

    /**
     * Write metadata.
     *
     * @param table   the table
     * @param blobId  the blob id
     * @param summary the summary
     */
    void writeMetadata(Table table, String blobId, StorageSummary summary);

    /**
     * Delete metadata.
     *  @param table  the table
     * @param blobId the blob id
     */
    void deleteMetadata(Table table, String blobId);

    /**
     * Count metadata.
     *
     * @param table  the table
     */
    long countMetadata(Table table);
}
