package com.bazaarvoice.emodb.sortedq.db;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

public interface QueueDAO {
    Iterator<String> listQueues();

    Map<UUID, String> loadSegments(String queue);

    /** Returns the minimum record for the specified segment. */
    @Nullable
    ByteBuffer findMinRecord(UUID dataId, @Nullable ByteBuffer from);

    /** Returns the maximum record for each of the specified segments. */
    Map<UUID, ByteBuffer> findMaxRecords(Collection<UUID> dataIds);

    /**
     * Returns the first {@code limit} records in a segment between {@code from} (inclusive) and {@code to}
     * (exclusive).  Internally it will query {@code batchSize} records at a time.  Callers should set
     * {@code batchSize} to a value that limits the number of records in memory at a time to something reasonable.
     */
    Iterator<ByteBuffer> scanRecords(UUID dataId, @Nullable ByteBuffer from, @Nullable ByteBuffer to,
                                     int batchSize, int limit);

    UpdateRequest prepareUpdate(String queue);

    interface UpdateRequest {
        UpdateRequest writeSegment(UUID segment, String internalState);

        UpdateRequest deleteSegment(UUID segment, UUID dataId);

        UpdateRequest writeRecords(UUID dataId, Collection<ByteBuffer> records);

        UpdateRequest deleteRecords(UUID dataId, Collection<ByteBuffer> records);

        void execute();
    }
}
