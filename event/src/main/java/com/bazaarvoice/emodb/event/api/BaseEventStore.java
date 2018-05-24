package com.bazaarvoice.emodb.event.api;

import com.google.common.base.Predicate;
import com.google.common.collect.Multimap;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Persistent collection of events, organized into channels.
 */
public interface BaseEventStore {

    Iterator<String> listChannels();

    void add(String channel, ByteBuffer event);

    void addAll(String channel, Collection<ByteBuffer> events);

    void addAll(Multimap<String, ByteBuffer> eventsByChannel);

    long getSizeEstimate(String channel, long limit);

    long getClaimCount(String channel);

    Map<String, Long> snapshotClaimCounts();

    List<EventData> peek(String channel, int limit);

    /** Returns true if there might be more events that could have been returned, if not for the sink limit. */
    boolean peek(String channel, EventSink sink);

    List<EventData> poll(String channel, Duration claimTtl, int limit);

    /** Returns true if there might be events that could have been returned, if not for the sink limit. */
    boolean poll(String channel, Duration claimTtl, EventSink sink);

    void renew(String channel, Collection<String> eventIds, Duration claimTtl, boolean extendOnly);

    void delete(String channel, Collection<String> eventIds, boolean cancelClaims);

    /**
     * Copies events matching the specified predicate from one channel to another.
     * If a non-null "since" timestamp is provided, only events since that time will be copied.
     */
    void copy(String from, String to, Predicate<ByteBuffer> filter, Date since);

    /** Moves all events from one channel to another. */
    void move(String from, String to);

    void unclaimAll(String channel);

    void purge(String channel);
}
