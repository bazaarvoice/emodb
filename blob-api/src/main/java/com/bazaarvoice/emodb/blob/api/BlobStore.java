package com.bazaarvoice.emodb.blob.api;

import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.TableOptions;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.google.common.io.InputSupplier;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public interface BlobStore {

    /**
     * Retrieves metadata about up to {@code limit} tables.  The records will be returned in a deterministic
     * order but not sorted alphabetically by key.  If {@code fromTableExclusive} is not null, the list will
     * start with the first table that follows the specified key in the returned sort order.
     * <p>
     * Note: clients are strongly encouraged to use the version of this method wrapped by the
     * {@code com.bazaarvoice.emodb.sor.client.DataStoreStreaming} class which will restart the iterator in the
     * event that the connection to the EmoDB server is lost while streaming results.
     */
    Iterator<Table> listTables(@Nullable String fromTableExclusive, long limit);

    /**
     * Creates a logical table in the data store.
     *
     * @throws TableExistsException if the table exists already with different options or a different attributes.
     */
    void createTable(String table, TableOptions options, Map<String, String> attributes, Audit audit)
            throws TableExistsException;

    /** Drops the specified table and all data associated with it. */
    void dropTable(String table, Audit audit)
            throws UnknownTableException;

    /**
     * Makes a best effort to permanently delete all data from the specified table.
     * <p/>
     * Useful for debugging/testing, but not intended for use in production because it performs an unbounded amount of work.
     */
    void purgeTableUnsafe(String table, Audit audit)
            throws UnknownTableException;

    /** Returns true if the specified table exists. */
    boolean getTableExists(String table);

    /** Returns true if the specified table is available to the current data center. */
    boolean isTableAvailable(String table);

    /** Returns the table metadata. */
    Table getTableMetadata(String table);

    /** Returns the attributes associated with a table. */
    Map<String, String> getTableAttributes(String table)
            throws UnknownTableException;

    /** Replaces the attributes for an existing table. */
    void setTableAttributes(String table, Map<String, String> attributes, Audit audit)
            throws UnknownTableException;

    /** Returns the storage options associated with a table. */
    TableOptions getTableOptions(String table)
            throws UnknownTableException;

    /**
     * Returns an estimate of the number of blobs stored in the specified table.  This will not include deleted blobs.
     * <p/>
     * Useful for debugging/testing, but not intended for use in production because it performs an unbounded amount of work.
     * <em>Please do not use this method to monitor your data in production!</em>
     */
    long getTableApproximateSize(String table)
            throws UnknownTableException;


    BlobMetadata getMetadata(String table, String blobId) throws BlobNotFoundException;

    /**
     * Retrieves metadata for up to {@code limit} objects from the specified table.  The records will be returned in a
     * deterministic order but not sorted alphabetically by key.  If {@code fromKeyExclusive} is not null, the scan
     * will start with the first record that follows the specified key in the returned sort order.
     * <p>
     * Note: clients are strongly encouraged to use the version of this method wrapped by the
     * {@code com.bazaarvoice.emodb.blob.client.BlobStoreStreaming} class which will restart the iterator in the
     * event that the connection to the EmoDB server is lost while streaming results.
     */
    Iterator<BlobMetadata> scanMetadata(String table, @Nullable String fromBlobIdExclusive, long limit);

    Blob get(String table, String blobId) throws BlobNotFoundException;

    Blob get(String table, String blobId, @Nullable RangeSpecification rangeSpec)
            throws BlobNotFoundException, RangeNotSatisfiableException;

    void put(String table, String blobId, InputSupplier<? extends InputStream> in, Map<String, String> attributes)
            throws IOException;

    void delete(String table, String blobId);

    /**
     * Returns list of valid table placements
     */
    Collection<String> getTablePlacements();
}
