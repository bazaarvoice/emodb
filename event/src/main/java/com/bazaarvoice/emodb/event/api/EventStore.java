package com.bazaarvoice.emodb.event.api;

import com.google.common.base.Predicate;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;

/**
 * Persistent collection of events, organized into channels.
 */
public interface EventStore extends BaseEventStore {

    /**
     * Equivalent to {@code addAll(channel, events)} followed immediately by {@code peek(channel, limit)}, but likely
     * more efficient.  The events added will be passed to the sink until the sink limit is satisfied.
     */
    boolean addAllAndPeek(String channel, Collection<ByteBuffer> events, EventSink sink);

    /**
     * Equivalent to {@code addAll(channel, events)} followed immediately by {@code poll(channel, claimTtl, limit)},
     * but likely more efficient.  The events added will be passed to the sink until the sink limit is satisfied.
     */
    boolean addAllAndPoll(String channel, Collection<ByteBuffer> events, Duration claimTtl, EventSink sink);

    /** Reads all events in the channel, passing them to the specified sink {@code batchSize} at a time. */
    void scan(String channel, Predicate<ByteBuffer> filter, ScanSink sink, int batchSize, Date since);
}
