package com.bazaarvoice.emodb.sor.db;

import com.bazaarvoice.emodb.sor.api.Compaction;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.api.WriteConsistency;
import com.bazaarvoice.emodb.sor.delta.Delta;
import com.bazaarvoice.emodb.table.db.Table;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public interface DataWriterDAO {
    /**
     * Returns the timestamp for which the data store has achieved full consistency
     * across all writers (across all data centers).  All writes older than this
     * timestamp are visible everywhere.  It should always be safe to compact deltas
     * that are older than this. This includes a *minimum lag* if provided, to guard
     * against too frequent compactions.
     */
    long getFullConsistencyTimestamp(Table table);

    /**
     * Same as {@link #getFullConsistencyTimestamp(com.bazaarvoice.emodb.table.db.Table)}
     * except it does not include minimum lag, but the actual last good FCT.
     * Use this carefully. In practice, this is only used for efficient clean up of
     * "compaction-owned" deltas, once the said compaction is visible everywhere.
     */
    long getRawConsistencyTimestamp(Table table);

    /** Creates, modifies or deletes a collection of data items in the data store. */
    void updateAll(Iterator<RecordUpdate> updates, UpdateListener listener);

    /** Deletes one or more deltas and replaces them with the specified update. */
    void compact(Table table, String key, UUID compactionKey, Compaction compaction, UUID changeId, Delta delta,
                 Collection<UUID> changesToDelete, List<History> historyList, WriteConsistency consistency);

    /** Writes delta audits. */
    void storeCompactedDeltas(Table tbl, String key, List<History> audits, WriteConsistency consistency);

    /** Makes a best effort to delete all data within the specified table.  Resets all version numbers to zero. */
    void purgeUnsafe(Table table);

    interface UpdateListener {
        void beforeWrite(Collection<RecordUpdate> updates);
    }
}
