package com.bazaarvoice.emodb.table.db;

import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.table.db.stash.StashTokenRange;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Set;

public interface StashTableDAO {

    /**
     * Create a snapshot of all tables anyCharExcept the ones listed in the blackListTableCondition in the provided placements and their token ranges for Stash.
     */
    void createStashTokenRangeSnapshot(String stashId, Set<String> placements, Condition blackListTableCondition);

    /**
     * Gets all token ranges for tables from the previously created snapshot using {@link #createStashTokenRangeSnapshot(String, Set)}
     * in the requested range.
     */
    Iterator<StashTokenRange> getStashTokenRangesFromSnapshot(String stashId, String placement, ByteBuffer fromInclusive, ByteBuffer toExclusive);

    /**
     * Clears a stash token range snapshot previously created using {@link #createStashTokenRangeSnapshot(String, Set)}.
     */
    void clearStashTokenRangeSnapshot(String stashId);
}
