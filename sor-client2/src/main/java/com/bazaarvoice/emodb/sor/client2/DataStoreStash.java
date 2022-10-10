package com.bazaarvoice.emodb.sor.client2;

import com.amazonaws.ClientConfiguration;
import com.bazaarvoice.emodb.common.stash.StandardStashReader;
import com.bazaarvoice.emodb.common.stash.StashReader;
import com.bazaarvoice.emodb.common.stash.StashRowIterable;
import com.bazaarvoice.emodb.common.stash.StashRowIterator;
import com.bazaarvoice.emodb.common.stash.StashSplit;
import com.bazaarvoice.emodb.common.stash.StashTable;
import com.bazaarvoice.emodb.common.stash.StashTableMetadata;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.StashNotAvailableException;
import com.bazaarvoice.emodb.sor.api.TableNotStashedException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;

import java.text.ParseException;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class provides client access to scan tables and get table splits from Stash.  Getting data from Stash is faster
 * than getting data directly from the DataStore and, since the data is served from S3, is not subject to the same
 * rate limiting.  The trade-off is that Stash data is generated daily and therefore may be up to a day out of sync.
 *
 * For clients using Stash to bootstrap a service they should follow the following pattern:
 *
 * <ol>
 *     <li>Create a databus subscription and start listening for databus events.</li>
 *     <li>Request a replay of the past two days for the subscription.  This will pick up any updates that took place
 *         since the Stash generation started.</li>
 *     <li>Use the Stash splits to bootstrap tables.</li>
 * </ol>
 *
 * <b>Caution</b>
 *
 * If you start processing the subscription while reading the Stash splits it is possible that you will receive a
 * record on the databus and then at a later time read the same record from Stash.  The Stash record will be
 * <i>older</i> than the databus record and therefore should not replace the databus version.  Here are two suggested
 * methods to prepare for this circumstance:
 *
 * <ol>
 *     <li>Perform a version check on the Stash record against any existing record prior to persisting it.</li>
 *     <li>Defer processing the databus until all Stash tables have been bootstrapped.</li>
 * </ol>
 */
public class DataStoreStash {

    private final DataStore _dataStore;
    private final StandardStashReader _stashReader;

    public static DataStoreStash getInstance(DataStore dataStore) {
        return new DataStoreStash(dataStore, StandardStashReader.getInstance(dataStore.getStashRoot()));
    }

    @Beta
    public static DataStoreStash getInstance(DataStore dataStore, ClientConfiguration s3Config) {
        return new DataStoreStash(dataStore, StandardStashReader.getInstance(dataStore.getStashRoot(), s3Config));
    }

    public static DataStoreStash getInstance(DataStore dataStore, String accessKey, String secretKey) {
        return new DataStoreStash(dataStore, StandardStashReader.getInstance(dataStore.getStashRoot(), accessKey,
                secretKey));
    }

    @Beta
    public static DataStoreStash getInstance(DataStore dataStore, String accessKey, String secretKey, ClientConfiguration s3Config) {
        return new DataStoreStash(dataStore, StandardStashReader.getInstance(dataStore.getStashRoot(), accessKey,
                secretKey, s3Config));
    }

    @VisibleForTesting
    DataStoreStash(DataStore dataStore, StandardStashReader stashReader) {
        _dataStore = dataStore;
        _stashReader = stashReader;
    }

    /**
     * Gets the time that is used as the stash folder name that will be used for subsequent scan and split operations.
     * To get the exact timestamp of when the stash actually started the scan use {@link #getStashStartTime()}.
     * By default this will be the time of the latest Stash available, but it can be locked using {@link #lockStashTime()}.
     */
    public Date getStashTime()
            throws StashNotAvailableException {
        return _stashReader.getLatestCreationTime();
    }

    /**
     * Gets the *exact* start time when stash started the scan. The Stash will contain all content up to that time
     * but is not guaranteed to contain any changes made after that time.
     */
    public Date getStashStartTime() throws ParseException {
        return _stashReader.getStashCreationTime();
    }

    /**
     * By default scan and splits calls will use the latest Stash available.  For long operations the caller may wish to
     * have a consistent view across multiple tables.  By calling this method all future Stash calls will use the
     * whatever the latest Stash was at the time this method was called.  This can be then be disabled by subsequently
     * calling {@link #unlockStashTime()}.
     */
    public void lockStashTime()
            throws StashNotAvailableException {
        _stashReader.lockToLatest();
    }

    /**
     * This method is like {@link #lockStashTime()} except the caller can request a specific Stash time, typically
     * one returned by a prior call to {@link #getStashTime()}.  If no Stash is available for the given time it will
     * raise a StashNotAvailableException.  This can be disabled by subsequently calling {@link #unlockStashTime()}.
     */
    public void lockStashTime(Date stashTime)
            throws StashNotAvailableException {
        _stashReader.lockToStashCreatedAt(stashTime);
    }

    /**
     * This method undoes Stash locking begun by {@link #lockStashTime()} or {@link #lockStashTime(Date)}.
     * All subsequent scans and splits will attempt to use the latest Stash available.
     */
    public void unlockStashTime()
            throws StashNotAvailableException {
        _stashReader.unlock();
    }

    /**
     * Returns true if the specified table exists.
     */
    public boolean getTableExists(String table) {
        return _stashReader.getTableExists(table);
    }

    /**
     * Lists all tables present in Stash.  Note that tables that were present in EmoDB but empty during the
     * Stash operation are not listed.
     */
    public Iterable<StashTable> listStashTables()
            throws StashNotAvailableException {
        final StashReader stashReader = _stashReader.getLockedView();

        return new Iterable<StashTable>() {
            @Override
            public Iterator<StashTable> iterator() {
                return stashReader.listTables();
            }
        };
    }

    /**
     * Lists the names of all tables present in Stash.  This is a convenience wrapper around {@link #listStashTables()}
     * that returns the result of calling {@link StashTable#getTableName()} on each record returned.
     *
     */
    public Iterable<String> listStashTableNames()
            throws StashNotAvailableException {
        return Iterables.transform(listStashTables(), new Function<StashTable, String>() {
            @Override
            public String apply(StashTable stashTable) {
                return stashTable.getTableName();
            }
        });
    }

    /**
     * Lists metadata about all tables present in Stash, including metadata about each file present in Stash.
     * Although this call provides some of the same information as {@link #listStashTables()} it is significantly
     * heavier weight.  If you don't need information about the individual files in each table it is more efficient
     * to use {@link #listStashTables()} instead.
     */
    public Iterable<StashTableMetadata> listStashTableMetadata()
            throws StashNotAvailableException {
        final StashReader stashReader = _stashReader.getLockedView();

        return new Iterable<StashTableMetadata>() {
            @Override
            public Iterator<StashTableMetadata> iterator() {
                return stashReader.listTableMetadata();
            }
        };
    }

    /**
     * Returns metadata about the specified table.  Generally it is preferable to use {@link #getSplits(String)}
     * if the only purpose for getting the metadata is to subsequently read the Stash content from S3.
     * However, if you need to know the the table metadata for another purpose, such as to supply input files
     * to a map-reduce job, then this method is appropriate.
     */
    public StashTableMetadata getStashTableMetadata(String table)
            throws StashNotAvailableException, TableNotStashedException {
        return _stashReader.getTableMetadata(table);
    }

    /**
     * Scans all rows from Stash for the given table.
     */
    public StashRowIterable scan(final String table)
            throws StashNotAvailableException, TableNotStashedException {
        try {
            final StashReader stashReader = _stashReader.getLockedView();

            return new StashRowIterable() {
                @Override
                protected StashRowIterator createStashRowIterator() {
                    return stashReader.scan(table);
                }
            };
        } catch (TableNotStashedException e) {
            throw propagateTableNotStashed(e);
        }
    }

    /**
     * Gets the splits from Stash for the given table.
     */
    public Collection<String> getSplits(String table)
            throws StashNotAvailableException, TableNotStashedException {
        try {
            return FluentIterable.from(_stashReader.getSplits(table))
                    .transform(Functions.toStringFunction())
                    .toList();
        } catch (TableNotStashedException e) {
            throw propagateTableNotStashed(e);
        }
    }

    /**
     * Gets the split content from a previous call to {@link #getSplits(String)}.  Note that this method is less
     * flexible than its DataStore counterpart in that, since the splits are pre-computed, the split always begins at
     * the first record.
     *
     * The splits returned will always match the Stash state from when the splits were originally returned by
     * {@link #getSplits(String)}.  Even if a newer Stash is subsequently generated the contents returned by
     * this method will come from the original Stash and not the newly generated one.
     */
    public StashRowIterable getSplit(String table, String split) {
        final StashSplit stashSplit = StashSplit.fromString(split);
        checkArgument(stashSplit.getTable().equals(table));

        return new StashRowIterable() {
            @Override
            protected StashRowIterator createStashRowIterator() {
                return _stashReader.getSplit(stashSplit);
            }
        };
    }

    /**
     * If a table exists but is empty then it will not be stashed.  This method will surface this exception
     * only if the table exists.  Otherwise it converts it to an UnknownTableException.
     */
    private RuntimeException propagateTableNotStashed(TableNotStashedException e)
            throws TableNotStashedException, UnknownTableException {
        if (_dataStore.getTableExists(e.getTable())) {
            throw e;
        }
        throw new UnknownTableException(e.getTable());
    }
}
