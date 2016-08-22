package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.common.api.impl.TimePartitioningIterator;
import com.bazaarvoice.emodb.common.json.RestartingStreamingIterator;
import com.bazaarvoice.emodb.common.json.StreamingIteratorSupplier;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.Table;
import com.bazaarvoice.emodb.sor.api.Update;
import com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wraps the streaming APIs in the {@link DataStore} with logic that automatically restarts the iterator in the
 * event that the connection to the EmoDB server is lost while streaming results.  This complements the Ostrich
 * retry logic and extends it to support long-lived streaming connections to EmoDB.
 */
public abstract class DataStoreStreaming {

    /** Prevent instantiation. */
    private DataStoreStreaming() {}

    /**
     * Retrieves metadata about all DataStore tables.
     */
    public static Iterable<Table> listTables(DataStore dataStore) {
        return listTables(dataStore, null, Long.MAX_VALUE);
    }

    /**
     * Retrieves all records from the specified table.
     */
    public static Iterable<Map<String, Object>> scan(DataStore dataStore,
                                                     String table,
                                                     ReadConsistency consistency) {
        return scan(dataStore, table, null, Long.MAX_VALUE, consistency);
    }

    /**
     * Retrieves all records from the specified split in the specified table.
     */
    public static Iterable<Map<String, Object>> getSplit(DataStore dataStore,
                                                         String table,
                                                         String split,
                                                         ReadConsistency consistency) {
        return getSplit(dataStore, table, split, null, Long.MAX_VALUE, consistency);
    }

    /**
     * Retrieves metadata about up to {@code limit} tables.  The records will be returned in a deterministic
     * order but not sorted alphabetically by key.  If {@code fromTableExclusive} is not null, the list will
     * start with the first table that follows the specified key in the returned sort order.
     */
    public static Iterable<Table> listTables(final DataStore dataStore,
                                             final @Nullable String fromTableExclusive,
                                             final long limit) {
        return RestartingStreamingIterator.stream(fromTableExclusive, limit,
                new StreamingIteratorSupplier<Table, String>() {
                    @Override
                    public Iterator<Table> get(String fromToken, long limit) {
                        return dataStore.listTables(fromToken, limit);
                    }

                    @Override
                    public String getNextToken(Table table) {
                        return table.getName();
                    }
                });
    }

    /**
     * Retrieves up to {@code limit} records from the specified table.  The records will be returned in a deterministic
     * order but not sorted alphabetically by key.  If {@code fromKeyExclusive} is not null, the scan will start with
     * the first record that follows the specified key in the returned sort order.
     */
    public static Iterable<Map<String, Object>> scan(final DataStore dataStore,
                                                     final String table,
                                                     final @Nullable String fromKeyExclusive,
                                                     final long limit,
                                                     final ReadConsistency consistency) {
        return RestartingStreamingIterator.stream(fromKeyExclusive, limit,
                new StreamingIteratorSupplier<Map<String, Object>, String>() {
                    @Override
                    public Iterator<Map<String, Object>> get(String fromToken, long limit) {
                        return dataStore.scan(table, fromToken, limit, consistency);
                    }

                    @Override
                    public String getNextToken(Map<String, Object> object) {
                        return Intrinsic.getId(object);
                    }
                });
    }

    /**
     * Retrieves up to {@code limit} records from the specified split in the specified table.  Like {@link #scan}, the
     * records will be returned in a deterministic order but not sorted alphabetically by key.  If
     * {@code fromKeyExclusive} is not null, the scan will start with the first record that follows the specified key
     * in the returned sort order.
     */
    public static Iterable<Map<String, Object>> getSplit(final DataStore dataStore,
                                                         final String table,
                                                         final String split,
                                                         final @Nullable String fromKeyExclusive,
                                                         final long limit,
                                                         final ReadConsistency consistency) {
        return RestartingStreamingIterator.stream(fromKeyExclusive, limit,
                new StreamingIteratorSupplier<Map<String, Object>, String>() {
                    @Override
                    public Iterator<Map<String, Object>> get(String fromToken, long limit) {
                        return dataStore.getSplit(table, split, fromToken, limit, consistency);
                    }

                    @Override
                    public String getNextToken(Map<String, Object> object) {
                        return Intrinsic.getId(object);
                    }
                });
    }

    /**
     * Creates, updates or deletes zero or more pieces of content in the data store.
     */
    public static void updateAll(DataStore dataStore, Iterable<Update> updates) {
        updateAll(dataStore, updates.iterator(), ImmutableSet.<String>of());
    }

    /**
     * Creates, updates or deletes zero or more pieces of content in the data store.
     * You can attach a set of databus event tags for these updates
     */
    public static void updateAll(DataStore dataStore, Iterable<Update> updates, Set<String> tags) {
        updateAll(dataStore, updates.iterator(), tags);
    }

    public static void updateAll(DataStore dataStore, Iterator<Update> updateIter) {
        updateAll(dataStore, updateIter, ImmutableSet.<String>of());
    }

    /**
     * Creates, updates or deletes zero or more pieces of content in the data store.
     * @param tags - Set of Strings or "tags" that would be attached to databus events generated for the updates
     */
    public static void updateAll(DataStore dataStore, Iterator<Update> updateIter, Set<String> tags) {
        // Invoke dataStore.updateAll() with smallish batches so if one batch fails due to server failure, we will
        // fail over to another server using the Ostrich retry/failover infrastructure and continue from the point
        // of the failure.
        // Use time-based partitioning that adjusts batch sizes dynamically in an effort to settle on a batches that
        // take 500 milliseconds to execute.  This should make the TimeLimitedIterator used by the DataStoreClient
        // unnecessary, but the TimeLimitedIterator is still relevant for clients that don't use DataStoreStreaming.
        // For now, hard-code initial/min/max/goal values.
        Iterator<List<Update>> batchIter =
                new TimePartitioningIterator<>(updateIter, 50, 1, 2500, Duration.millis(500L));
        while (batchIter.hasNext()) {
            // Ostrich will retry each batch as necessary
            dataStore.updateAll(batchIter.next(), tags);
        }
    }

    /**
     * Creates, updates or deletes zero or more pieces of content in the data store facades.
     */
    public static void updateAllForFacade(DataStore dataStore, Iterable<Update> updates) {
        updateAllForFacade(dataStore, updates.iterator());
    }

    public static void updateAllForFacade(DataStore dataStore, Iterator<Update> updateIter) {
        updateAllForFacade(dataStore, updateIter, ImmutableSet.<String>of());
    }

    /**
     * Creates, updates or deletes zero or more pieces of content in the data store facades.
     */
    public static void updateAllForFacade(DataStore dataStore, Iterator<Update> updateIter, Set<String> tags) {
        // Invoke dataStore.updateAll() with smallish batches so if one batch fails due to server failure, we will
        // fail over to another server using the Ostrich retry/failover infrastructure and continue from the point
        // of the failure.
        // Use time-based partitioning that adjusts batch sizes dynamically in an effort to settle on a batches that
        // take 500 milliseconds to execute.  This should make the TimeLimitedIterator used by the DataStoreClient
        // unnecessary, but the TimeLimitedIterator is still relevant for clients that don't use DataStoreStreaming.
        // For now, hard-code initial/min/max/goal values.
        Iterator<List<Update>> batchIter =
                new TimePartitioningIterator<>(updateIter, 50, 1, 2500, Duration.millis(500L));
        while (batchIter.hasNext()) {
            // Ostrich will retry each batch as necessary
            dataStore.updateAllForFacade(batchIter.next(), tags);
        }
    }
}
