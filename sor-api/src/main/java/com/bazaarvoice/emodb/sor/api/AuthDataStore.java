package com.bazaarvoice.emodb.sor.api;

import com.bazaarvoice.emodb.auth.proxy.Credential;
import com.bazaarvoice.emodb.sor.delta.Delta;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Authenticating interface to {@link DataStore}.  This method should exactly mirror DataStore except with added credentials
 * in each call.
 * @see DataStore
 */
public interface AuthDataStore {
    /**
     * Retrieves metadata about up to {@code limit} tables.  The records will be returned in a deterministic
     * order but not sorted alphabetically by key.  If {@code fromTableExclusive} is not null, the list will
     * start with the first table that follows the specified key in the returned sort order.
     * <p>
     * Note: clients are strongly encouraged to use the version of this method wrapped by the
     * {@code com.bazaarvoice.emodb.sor.client.DataStoreStreaming} class which will restart the iterator in the
     * event that the connection to the EmoDB server is lost while streaming results.
     */
    Iterator<Table> listTables(@Credential String apiKey, @Nullable String fromTableExclusive, long limit);

    /**
     * Creates a logical table in the data store.
     *
     * @throws TableExistsException if the table exists already with different options or a different template.
     */
    void createTable(@Credential String apiKey, String table, TableOptions options, Map<String, ?> template, Audit audit)
            throws TableExistsException;

    /**
     * Drops the specified table and all data associated with it.
     */
    void dropTable(@Credential String apiKey, String table, Audit audit)
            throws UnknownTableException;

    /**
     * Makes a best effort to permanently delete all data from the specified table.  This method is not safe for use
     * with the databus--it does not generate databus events and resets the version number of all content to zero.
     * Useful for debugging/testing, but not intended for use in production.
     */
    void purgeTableUnsafe(@Credential String apiKey, String table, Audit audit)
            throws UnknownTableException;

    /**
     * Returns true if the specified table exists.
     */
    boolean getTableExists(@Credential String apiKey, String table);

    /**
     * Returns true if the specified table is available to the current data center.
     */
    boolean isTableAvailable(@Credential String apiKey, String table);

    /**
     * Returns the table metadata.
     */
    Table getTableMetadata(@Credential String apiKey, String table);

    /**
     * Returns the template associated with a table.
     */
    Map<String, Object> getTableTemplate(@Credential String apiKey, String table)
            throws UnknownTableException;

    /**
     * Replaces the template for an existing table.
     */
    void setTableTemplate(@Credential String apiKey, String table, Map<String, ?> template, Audit audit)
            throws UnknownTableException;

    /**
     * Returns the storage options associated with a table.
     */
    TableOptions getTableOptions(@Credential String apiKey, String table)
            throws UnknownTableException;

    /**
     * Returns an estimate of the number of distinct records ever written to the specified table.  This will include
     * records that have been deleted by writing a Delete delta.
     * <p/>
     * Useful for debugging/testing, but not intended for use in production because it performs an unbounded amount of work.
     * <em>Please do not use this method to monitor your data in production!</em>
     */
    long getTableApproximateSize(@Credential String apiKey, String table) throws UnknownTableException;
    long getTableApproximateSize(@Credential String apiKey, String table, int limit) throws UnknownTableException;

    /**
     * Retrieves the current version of a piece of content from the data store.
     * Uses {@link ReadConsistency#STRONG}.
     */
    Map<String, Object> get(@Credential String apiKey, String table, String key);

    /**
     * Retrieves the current version of a piece of content from the data store.
     */
    Map<String, Object> get(@Credential String apiKey, String table, String key, ReadConsistency consistency);

    /**
     * Retrieves all recorded history for a piece of content in the data store.
     */
    Iterator<Change> getTimeline(@Credential String apiKey, String table, String key, boolean includeContentData,
                                 boolean includeAuditInformation, @Nullable UUID start, @Nullable UUID end,
                                 boolean reversed, long limit, ReadConsistency consistency);

    /**
     * Retrieves up to {@code limit} records from the specified table.  The records will be returned in a deterministic
     * order but not sorted alphabetically by key.  If {@code fromKeyExclusive} is not null, the scan will start with
     * the first record that follows the specified key in the returned sort order.
     * <p>
     * Note: clients are strongly encouraged to use the version of this method wrapped by the
     * {@code com.bazaarvoice.emodb.sor.client.DataStoreStreaming} class which will restart the iterator in the
     * event that the connection to the EmoDB server is lost while streaming results.
     */
    Iterator<Map<String, Object>> scan(@Credential String apiKey, String table, @Nullable String fromKeyExclusive,
                                       long limit, boolean includeDeletes, ReadConsistency consistency);

    /**
     * Returns a list of split identifiers that can be used to scan all records in the specified table in parallel using
     * the {@link #getSplit} method.  This method will make a best effort to return splits smaller than or equal to the
     * specified desired number of records per split.
     */
    Collection<String> getSplits(@Credential String apiKey, String table, int desiredRecordsPerSplit);

    /**
     * Retrieves up to {@code limit} records from the specified split in the specified table.  Like {@link #scan}, the
     * records will be returned in a deterministic order but not sorted alphabetically by key.  If
     * {@code fromKeyExclusive} is not null, the scan will start with the first record that follows the specified key
     * in the returned sort order.
     * <p>
     * Note: clients are strongly encouraged to use the version of this method wrapped by the
     * {@code com.bazaarvoice.emodb.sor.client.DataStoreStreaming} class which will restart the iterator in the
     * event that the connection to the EmoDB server is lost while streaming results.
     */
    Iterator<Map<String, Object>> getSplit(@Credential String apiKey, String table, String split,
                                           @Nullable String fromKeyExclusive, long limit, boolean includeDeletes,
                                           ReadConsistency consistency);

    /**
     * Retrieves records from the specified list of coordinates. The records will *not* be returned in the order it was
     * sent, and may not have any deterministic order.
     * Uses {@link ReadConsistency#STRONG}.
     */
    Iterator<Map<String, Object>> multiGet(@Credential String apiKey, List<Coordinate> coordinates);

    /**
     * Retrieves records from the specified list of coordinates. The records will *not* be returned in the order it was
     * sent, and may not have any deterministic order.
     */
    Iterator<Map<String, Object>> multiGet(@Credential String apiKey, List<Coordinate> coordinates, ReadConsistency consistency);

    /**
     * Creates, updates or deletes a piece of content in the data store.
     * Uses {@link WriteConsistency#STRONG}.
     */
    void update(@Credential String apiKey, String table, String key, UUID changeId, Delta delta, Audit audit);

    /**
     * Creates, updates or deletes a piece of content in the data store.
     */
    void update(@Credential String apiKey, String table, String key, UUID changeId, Delta delta, Audit audit, WriteConsistency consistency);

    /**
     * Creates, updates or deletes zero or more pieces of content in the data store.
     * <p>
     * Note: clients are strongly encouraged to use the version of this method wrapped by the
     * {@code com.bazaarvoice.emodb.sor.client.DataStoreStreaming} class which will avoid restarting the entire
     * iterable in the event that the connection to the EmoDB server is lost while streaming updates.
     */
    void updateAll(@Credential String apiKey, Iterable<Update> updates);

    /**
     * CAUTION: Using this method will make databus listeners inconsistent with the data, and would require them to rescan.
     * Creates, updates or deletes zero or more pieces of content in the data store *without* generating databus events.
     * You should generally avoid using this as it would make the databus listeners inconsistent with latest data.
     * In practice, this is used when an extremely big batch of updates will deluge the databus listeners in events
     * that they can't handle.
     * <p>
     * Note: clients are strongly encouraged to use the version of this method wrapped by the
     * {@code com.bazaarvoice.emodb.sor.client.DataStoreStreaming} class which will avoid restarting the entire
     * iterable in the event that the connection to the EmoDB server is lost while streaming updates.
     * @param updates - Iterable of updates
     * @param tags - Databus event tags for events generated by the updates
     */
    void updateAll(@Credential String apiKey, Iterable<Update> updates, Set<String> tags);

    /**
     * Attempts to reduce the size of the specified content in the underlying storage.
     */
    void compact(@Credential String apiKey, String table, String key, @Nullable Duration ttlOverride, ReadConsistency readConsistency, WriteConsistency writeConsistency);

    /**
     * Returns list of valid table placements
     */
    Collection<String> getTablePlacements(@Credential String apiKey);

    /** Facade related calls **/

    /**
     * Creates a logical facade for an existing table in the data store.
     *
     * @throws TableExistsException if:
     *  - the table does *not* exist - need a table to create a facade
     *  - the facade's placement overlap with existing facades' placements
     */
    void createFacade(@Credential String apiKey, String table, FacadeOptions options, Audit audit)
            throws TableExistsException;

    /**
     * Creates, updates or deletes zero or more pieces of content in the data store facade.
     * <p>
     * Note: clients are strongly encouraged to use the version of this method wrapped by the
     * {@code com.bazaarvoice.emodb.sor.client.DataStoreStreaming} class which will avoid restarting the entire
     * iterable in the event that the connection to the EmoDB server is lost while streaming updates.
     */
    void updateAllForFacade(@Credential String apiKey, Iterable<Update> updates);

    /**
     * CAUTION: Using this method will make databus listeners inconsistent with the data, and would require them to rescan.
     * Creates, updates or deletes zero or more pieces of content in the data store facade *without* generating databus events.
     * You should generally avoid using this as it would make the databus listeners inconsistent with latest data.
     * In practice, this is used when an extremely large batch of updates will deluge the databus listeners in events
     * that they can't handle.
     * <p>
     * Note: clients are strongly encouraged to use the version of this method wrapped by the
     * {@code com.bazaarvoice.emodb.sor.client.DataStoreStreaming} class which will avoid restarting the entire
     * iterable in the event that the connection to the EmoDB server is lost while streaming updates.
     * @param updates - Iterable of updates
     * @param tags - Databus event tags that will be attached to the events generated by the updates
     */
    void updateAllForFacade(@Credential String apiKey, Iterable<Update> updates, Set<String> tags);

    /**
     * Drops the specified facade and all data associated with it.
     */
    void dropFacade(@Credential String apiKey, String table, String placement, Audit audit)
            throws UnknownTableException;

    /**
     * Returns the root S3 stash path as a URI, such as "s3://emodb-us-east-1/stash/ci", or absent if
     * this instance server does not use Stash.
     */
    URI getStashRoot(@Credential String apiKey)
            throws StashNotAvailableException;
}
