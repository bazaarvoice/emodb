package com.bazaarvoice.emodb.sortedq.api;

import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;

public interface SortedQueue {
    boolean isReadOnly();

    /** Prevent all future modifications.  Once set, this cannot be unset. */
    void setReadOnly();

    /**
     * Adds the specified records to the queue.  If a record is added that is already present in the queue, the
     * duplicate is ignored.
     */
    void addAll(Collection<ByteBuffer> records);

    /**
     * Returns all records starting from the specified record, up to the specified limit.  Unlike the {@code drainTo}
     * method this always starts from the first record and does not wrap around.
     */
    Iterator<ByteBuffer> scan(@Nullable ByteBuffer fromInclusive, long limit);

    void drainTo(Consumer consumer, long limit);

    /**
     * Reads all records in the queue, passes them to the specified consumer, then deletes them, up to the specified
     * limit.  The process works in batches, so the consumer may be called multiple times, once per batch.  The batch
     * size is not configurable.  The {@code drainTo} starts reading at an arbitrary position in the queue, but within
     * a single call records will be returned in sorted order, wrapping around if necessary.  In general, each call
     * to {@code drainTo} starts where the previous call left off, so a sequence of calls will loop around all items
     * in the queue in a fair manner.
     */
    void drainTo(Consumer consumer, long limit, @Nullable Duration timeout);

    /** Removes all records from the queue. */
    void clear();

    /**
     * Returns an estimate of the number of distinct records in the queue.  The estimate is usually within 10% of
     * the actual value, and is guaranteed to be accurate with respect to whether or not the queue is empty (ie. it
     * always returns zero if the queue is empty and it never returns zero if the queue is not empty), subject to
     * concurrent {@code addAll} and {@code drainTo} operations.
     */
    long sizeEstimate();

    /** Returns true if there are no records in the queue. */
    boolean isEmpty();
}
