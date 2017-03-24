package com.bazaarvoice.emodb.event.api;

import com.google.common.base.Predicate;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Date;

/**
 * An extension of {@link BaseEventStore} that makes a good faith effort to remove duplicates from the event stream.
 * <p>
 * It exposes mostly same methods as {@link EventStore}.
 */
public interface DedupEventStore extends BaseEventStore {

    /**
     * Copies events matching the specified predicate from a non-dedup'd channel to a dedup'd channel.
     * If a non-null "since" timestamp is non-null, only events since that time will be copied.
     */
    void copyFromRawChannel(String from, String to, Predicate<ByteBuffer> filter, Date since, @Nullable EventTracer tracer);

    /** Moves all data from a dedup'd channel to a non-dedup'd channel. */
    void moveToRawChannel(String from, String to, @Nullable EventTracer tracer);
}
