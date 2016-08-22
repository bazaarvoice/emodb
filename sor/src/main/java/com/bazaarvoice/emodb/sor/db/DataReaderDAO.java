package com.bazaarvoice.emodb.sor.db;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.sor.api.Change;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.table.db.Table;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.google.common.base.Optional;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

public interface DataReaderDAO {
    /** Returns the number of non-deleted rows in the specified table. */
    long count(Table table, ReadConsistency consistency);
    long count(Table table, @Nullable Integer limit, ReadConsistency consistency);

    /** Retrieves all delta information for a single piece of content in the data store. */
    Record read(Key key, ReadConsistency consistency);

    /** Retrieves all delta information for a collection of records.  Returns records in arbitrary order. */
    Iterator<Record> readAll(Collection<Key> keys, ReadConsistency consistency);

    /** Retrieves history for a single piece of content using the specified range restrictions. */
    Iterator<Change> readTimeline(Key key, boolean includeContentData, boolean includeAuditInformation,
                                  UUID start, UUID end, boolean reversed, long limit, ReadConsistency consistency);

    /** Retrieves history of audit records using the specified range restrictions. */
    Iterator<Change> getExistingAudits(Key key, UUID start, UUID end, ReadConsistency consistency);

    /** Retrieves up to {@code limit} records from the specified table. */
    Iterator<Record> scan(Table table, @Nullable String fromKeyExclusive, LimitCounter limit, ReadConsistency consistency);

    /**
     * Returns a list of split identifiers that, when queried, will return all data in the table.  This method will
     * make a best effort to return splits smaller than or equal to the specified desired number of records per split.
     */
    List<String> getSplits(Table table, int desiredRecordsPerSplit);

    /** Retrieves up to {@code limit} records from the specified split in the specified table. */
    Iterator<Record> getSplit(Table table, String split, @Nullable String fromKeyExclusive, LimitCounter limit, ReadConsistency consistency);

    /**
     * Like {@link #getSplits(com.bazaarvoice.emodb.table.db.Table, int)} for an entire placement.  Useful in conjunction
     * with a multi-table scan for scanning subranges of a token range.  Returns a ScanRangeSplits which provides
     * splits by groups of hosts.  Querying each group's hosts sequentially should evenly distribute the load.
     * The optional "subrange" parameter limits the splits only to the matching token range.
     */
    ScanRangeSplits getScanRangeSplits(String placement, int desiredRecordsPerSplit, Optional<ScanRange> subrange);

    /**
     * Returns a string identifying the backend cluster which serves this placement.  The only implication the caller
     * should infer is that if two placements return the same value then operations to tables in both placements will
     * utilize the same resources.
     */
    String getPlacementCluster(String placement);

    /** Retrieves records all records across multiple tables in their natural key order (shard, table UUID, key). */
    Iterator<MultiTableScanResult> multiTableScan(MultiTableScanOptions query, TableSet tables, LimitCounter limit,
                                                  ReadConsistency consistency);
}
