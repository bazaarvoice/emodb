package com.bazaarvoice.emodb.event.core;

import com.bazaarvoice.emodb.common.dropwizard.metrics.ParameterizedTimed;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.event.api.EventSink;
import com.bazaarvoice.emodb.event.api.EventStore;
import com.bazaarvoice.emodb.event.api.ScanSink;
import com.bazaarvoice.emodb.event.api.SimpleEventSink;
import com.bazaarvoice.emodb.event.db.EventId;
import com.bazaarvoice.emodb.event.db.EventIdSerializer;
import com.bazaarvoice.emodb.event.db.EventReaderDAO;
import com.bazaarvoice.emodb.event.db.EventWriterDAO;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class DefaultEventStore implements EventStore {
    private static final Logger _log = LoggerFactory.getLogger(DefaultEventStore.class);

    private static final Duration DELETE_CLAIM_TTL = Duration.ofMillis(25);
    private static final int MAX_COPY_LIMIT = 1000;

    // Don't log system channels--they floods the logs and drown out what's usually interesting, especially w/"__system_bus:master".
    private static final String SKIP_DEBUG_LOGGING_PREFIX1 = "__system";
    private static final String SKIP_DEBUG_LOGGING_PREFIX2 = "__dedupq_read:__system";

    private final EventReaderDAO _readerDao;
    private final EventWriterDAO _writerDao;
    private final EventIdSerializer _eventIdSerializer;
    private final ClaimStore _claimStore;
    private final Cache<String, Boolean> _emptyCache;

    @Inject
    public DefaultEventStore(EventReaderDAO readerDao, EventWriterDAO writerDao,
                             EventIdSerializer eventIdSerializer, ClaimStore claimStore) {
        _readerDao = checkNotNull(readerDao, "readerDao");
        _writerDao = checkNotNull(writerDao, "writerDao");
        _eventIdSerializer = checkNotNull(eventIdSerializer, "eventIdSerializer");
        _claimStore = checkNotNull(claimStore, "claimStore");
        _emptyCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.SECONDS).build();
    }

    @Override
    public Iterator<String> listChannels() {
        return _readerDao.listChannels();
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public void add(String channel, ByteBuffer event) {
        checkNotNull(channel, "channel");
        checkNotNull(event, "event");

        addAll(ImmutableMultimap.of(channel, event));
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public void addAll(String channel, Collection<ByteBuffer> events) {
        checkNotNull(channel, "channel");
        checkNotNull(events, "events");

        if (events.isEmpty()) {
            return;
        }

        addAll(toEventsByChannel(channel, events));
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public void addAll(Multimap<String, ByteBuffer> eventsByChannel) {
        checkNotNull(eventsByChannel, "eventsByChannel");

        if (eventsByChannel.isEmpty()) {
            return;
        }

        if (isDebugLoggingEnabled(eventsByChannel)) {
            _log.debug("addAll {}", Maps.transformValues(eventsByChannel.asMap(), new Function<Collection<ByteBuffer>, Integer>() {
                @Override
                public Integer apply(Collection<ByteBuffer> records) {
                    return records.size();
                }
            }));
        }

        _writerDao.addAll(eventsByChannel, null);
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public long getSizeEstimate(String channel, long limit) {
        checkNotNull(channel, "channel");
        checkLimit(limit, Long.MAX_VALUE);

        return _readerDao.count(channel, limit);
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public long getClaimCount(String channel) {
        checkNotNull(channel, "channel");

        return _claimStore.withClaimSet(channel, new Function<ClaimSet, Long>() {
            @Override
            public Long apply(ClaimSet claims) {
                return claims.size();
            }
        });
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public Map<String, Long> snapshotClaimCounts() {
        return _claimStore.snapshotClaimCounts();
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public List<EventData> peek(String channel, int limit) {
        SimpleEventSink sink = new SimpleEventSink(limit);
        peek(channel, sink);
        return sink.getEvents();
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public boolean peek(String channel, EventSink sink) {
        checkNotNull(channel, "channel");
        int limit = sink.remaining();
        checkLimit(limit, Limits.MAX_PEEK_LIMIT);

        DaoEventSink daoSink = new DaoEventSink(Integer.MAX_VALUE, sink);
        _readerDao.readAll(channel, daoSink, null, true);

        if (isDebugLoggingEnabled(channel)) {
            _log.debug("peek {} limit={} -> #={} more={}", channel, limit, daoSink.getCount(), daoSink.hasMore());
        }

        return daoSink.hasMore();
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public boolean addAllAndPeek(String channel, Collection<ByteBuffer> events, EventSink sink) {
        checkNotNull(channel, "channel");
        checkNotNull(events, "events");

        if (events.isEmpty()) {
            // No events were provided, so by definition there aren't any more
            return false;
        }

        DaoEventSink daoSink = new DaoEventSink(Integer.MAX_VALUE, sink);
        _writerDao.addAll(toEventsByChannel(channel, events), daoSink);

        if (isDebugLoggingEnabled(channel)) {
            _log.debug("addAllAndPeek {} count={} extra={}", channel, events.size(), events.size() - daoSink.getCount());
        }

        return daoSink.hasMore();
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public List<EventData> poll(String channel, Duration claimTtl, int limit) {
        SimpleEventSink sink = new SimpleEventSink(limit);
        poll(channel, claimTtl, sink);
        return sink.getEvents();
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public boolean poll(final String channel, final Duration claimTtl, final EventSink sink) {
        checkNotNull(channel, "channel");
        checkClaimTtl(claimTtl);
        final int limit = sink.remaining();
        checkLimit(limit, Limits.MAX_POLL_LIMIT);

        // If the channel is flagged empty in the cache, back off and simply return an empty list.
        // This increases the latency of events through the system but reduces load due to idle queues.
        if (Boolean.TRUE.equals(_emptyCache.getIfPresent(channel))) {
            return false;
        }

        return _claimStore.withClaimSet(channel, new Function<ClaimSet, Boolean>() {
            @Override
            public Boolean apply(ClaimSet claims) {
                // Protect from abusive clients that use long TTLs but never ack messages.  Don't allow more
                // than 10k claims outstanding per channel.  Don't error, just pretend no events are available.
                int hardLimit = getClaimsAllowed(claims, Limits.MAX_CLAIMS_OUTSTANDING);
                if (hardLimit <= 0) {
                    return false;
                }

                DaoEventSink daoSink = new DaoEventSink(channel, claims, claimTtl, hardLimit, sink);
                _readerDao.readNewer(channel, daoSink);

                // If all events have been read, cache this fact and expire it in 1 second.
                if (!daoSink.hasMore()) {
                    _emptyCache.put(channel, true);
                }

                if (isDebugLoggingEnabled(channel)) {
                    _log.debug("poll {} ttl={} limit={} -> #={} more={}",
                            channel, claimTtl, limit, daoSink.getCount(), daoSink.hasMore());
                }

                return daoSink.hasMore();
            }
        });
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public boolean addAllAndPoll(final String channel, final Collection<ByteBuffer> events,
                              final Duration claimTtl, final EventSink sink) {
        checkNotNull(channel, "channel");
        checkNotNull(events, "events");
        checkClaimTtl(claimTtl);

        if (events.isEmpty()) {
            // No events were provided, so by definition there aren't any more
            return false;
        }

        return _claimStore.withClaimSet(channel, new Function<ClaimSet, Boolean>() {
            @Override
            public Boolean apply(final ClaimSet claims) {
                // Protect from abusive clients that use long TTLs but never ack messages.
                int hardLimit = getClaimsAllowed(claims, Limits.MAX_CLAIMS_OUTSTANDING);

                DaoEventSink daoSink = new DaoEventSink(channel, claims, claimTtl, hardLimit, sink);
                _writerDao.addAll(toEventsByChannel(channel, events), daoSink);

                if (isDebugLoggingEnabled(channel)) {
                    _log.debug("addAllAndPoll {} count={} ttl={} extra={}",
                            channel, events.size(), claimTtl, events.size() - daoSink.getCount());
                }

                return daoSink.hasMore();
            }
        });
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public void renew(String channel, Collection<String> eventIds, final Duration claimTtl, final boolean extendOnly) {
        checkNotNull(channel, "channel");
        checkNotNull(eventIds, "eventIds");
        checkClaimTtl(claimTtl);

        if (eventIds.isEmpty() || (extendOnly && claimTtl.isZero())) {
            return;
        }

        if (isDebugLoggingEnabled(channel)) {
            _log.debug("renew {} count={} ttl={}", channel, eventIds.size(), claimTtl);
        }

        final Collection<EventId> eventIdObjects = toEventIds(eventIds, channel);

        _claimStore.withClaimSet(channel, new Function<ClaimSet, Void>() {
            @Override
            public Void apply(ClaimSet claims) {
                // Renew isn't supposed to create new claims, just update existing ones.  So don't enforce
                // MAX_CLAIMS_OUTSTANDING since it's complicated to implement correctly and we don't have a good
                // way to inform the caller that a claim renewal request was ignored.  Plus it's a lot less likely
                // that clients will abuse renew() compared to poll().

                claims.renewAll(toClaimIds(eventIdObjects), claimTtl, extendOnly);

                // If the claim TTL is zero then renewing these events effectively makes them available again immediately
                if (claimTtl.isZero()) {
                    markUnread(channel, eventIdObjects);
                }

                return null;
            }
        });
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public void delete(String channel, Collection<String> eventIds, boolean cancelClaims) {
        checkNotNull(channel, "channel");
        checkNotNull(eventIds, "eventIds");

        if (isDebugLoggingEnabled(channel)) {
            _log.debug("delete {} count={} cancel={}", channel, eventIds.size(), cancelClaims);
        }

        if (eventIds.isEmpty()) {
            return;
        }

        final Collection<EventId> eventIdObjects = toEventIds(eventIds, channel);

        _writerDao.delete(channel, eventIdObjects);

        if (cancelClaims) {
            // Don't delete the claims just yet.  Avoid race conditions with poll() by renewing the claims for
            // just a short while.
            _claimStore.withClaimSet(channel, new Function<ClaimSet, Void>() {
                @Override
                public Void apply(ClaimSet claims) {
                    claims.renewAll(toClaimIds(eventIdObjects), DELETE_CLAIM_TTL, false);
                    return null;
                }
            });
        }
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public void scan(String channel, Predicate<ByteBuffer> filter, ScanSink sink, int batchSize, Date since) {
        scanInternal(channel, filter, sink, batchSize, since);
    }

    private void scanInternal(String channel, final Predicate<ByteBuffer> filter,
                              final ScanSink sink, final int batchSize, Date since) {
        checkNotNull(channel, "channel");
        checkNotNull(filter, "filter");
        checkNotNull(sink, "sink");
        checkArgument(batchSize > 0, "batchSize must be >0");

        if (isDebugLoggingEnabled(channel)) {
            _log.debug("scan {}", channel);
        }
        final List<ByteBuffer> events = Lists.newArrayList();
        com.bazaarvoice.emodb.event.db.EventSink eventSink = new com.bazaarvoice.emodb.event.db.EventSink() {
            @Override
            public boolean accept(EventId eventId, ByteBuffer eventData) {
                if (filter.apply(eventData)) {
                    events.add(eventData);
                    if (events.size() >= batchSize) {
                        sink.accept(events);
                        events.clear();
                    }
                }
                return true;
            }
        };
        _readerDao.readAll(channel, eventSink, since, false);
        if (!events.isEmpty()) {
            sink.accept(events);
        }
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public void copy(String fromChannel, final String toChannel, final Predicate<ByteBuffer> filter, Date since) {
        checkNotNull(fromChannel, "fromChannel");
        checkNotNull(toChannel, "toChannel");
        checkNotNull(filter, "filter");

        if (isDebugLoggingEnabled(toChannel)) {
            _log.debug("copy from={} to={}", fromChannel, toChannel);
        }

        scanInternal(fromChannel, filter, new ScanSink() {
            @Override
            public void accept(List<ByteBuffer> events) {
                _writerDao.addAll(toEventsByChannel(toChannel, events), null);
            }
        }, MAX_COPY_LIMIT, since);
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public void move(final String fromChannel, final String toChannel) {
        checkNotNull(fromChannel, "fromChannel");
        checkNotNull(toChannel, "toChannel");

        if (fromChannel.equals(toChannel)) {
            return; // nothing to do
        }

        if (isDebugLoggingEnabled(toChannel)) {
            _log.debug("move from={} to={}", fromChannel, toChannel);
        }

        // Move all the data in "closed" slabs where we know are no race conditions with writers.  For queues with
        // lots and lots of data this is a fast way to move the bulk of the events from one channel to another.
        // Note that, because the slabId doesn't change and EventId embeds the slabId, "event=poll(from)" followed by
        // "move(from, to)" followed by "delete(event)" may delete from the "to" channel.  This race condition is
        // likely to be confusing but harmless so we're willing to live with it to get good move performance.
        boolean movedAll = _readerDao.moveIfFast(fromChannel, toChannel);
        if (movedAll) {
            return;
        }

        // For the events left over, move is implemented as a combination of copy+delete.
        final Collection<ByteBuffer> eventsToCopy = Lists.newArrayList();
        final Collection<EventId> eventsToDelete = Lists.newArrayList();
        _readerDao.readAll(fromChannel, new com.bazaarvoice.emodb.event.db.EventSink() {
            @Override
            public boolean accept(EventId eventId, ByteBuffer eventData) {
                eventsToCopy.add(eventData);
                eventsToDelete.add(eventId);
                if (eventsToCopy.size() >= MAX_COPY_LIMIT) {
                    addAndDelete(toChannel, eventsToCopy, fromChannel, eventsToDelete);
                    eventsToCopy.clear();
                    eventsToDelete.clear();
                }
                return true;
            }
        }, null, false);
        if (!eventsToCopy.isEmpty()) {
            addAndDelete(toChannel, eventsToCopy, fromChannel, eventsToDelete);
        }
    }

    private void addAndDelete(String addChannel, Collection<ByteBuffer> add,
                              String deleteChannel, Collection<EventId> delete) {
        _writerDao.addAll(toEventsByChannel(addChannel, add), null);
        _writerDao.delete(deleteChannel, delete);
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public void unclaimAll(String channel) {
        checkNotNull(channel, "channel");

        if (isDebugLoggingEnabled(channel)) {
            _log.debug("unclaimAll {}", channel);
        }

        _claimStore.withClaimSet(channel, new Function<ClaimSet, Void>() {
            @Override
            public Void apply(ClaimSet claims) {
                claims.clear();
                return null;
            }
        });
    }

    @ParameterizedTimed(type="DefaultEventStore")
    @Override
    public void purge(String channel) {
        checkNotNull(channel, "channel");

        if (isDebugLoggingEnabled(channel)) {
            _log.debug("purge {}", channel);
        }

        // Release all claims to reduce memory.
        _claimStore.withClaimSet(channel, new Function<ClaimSet, Void>() {
            @Override
            public Void apply(ClaimSet claims) {
                claims.clear();
                return null;
            }
        });

        // Delete events as best we can.  This isn't completely thread-safe w/regard to releasing the claims, but by
        // releasing claims first at worst we end up with a few unnecessary in-memory claims.
        _writerDao.deleteAll(channel);
    }

    private boolean claim(@Nullable ClaimSet claims, EventId eventId, Duration claimTtl) {
        return claims == null || (claimTtl.isZero() ?
                !claims.isClaimed(eventId.array()) :
                claims.acquire(eventId.array(), claimTtl));
    }

    private void unclaim(@Nullable ClaimSet claims, EventId eventId, Duration originalClaimTtl) {
        if (claims != null && originalClaimTtl.toMillis() > 0) {
            claims.renew(eventId.array(), Duration.ZERO, false);
        }
    }

    private void markUnread(@Nullable String channel, Collection<EventId> eventIds) {
        if (channel != null) {
            // Mark the events as unread
            _readerDao.markUnread(channel, eventIds);
            // Remove the channel from the empty cache, or no-op if it wasn't cached as empty
            _emptyCache.invalidate(channel);
        }
    }

    private int getClaimsAllowed(ClaimSet claims, int maxClaimsOutstanding) {
        return maxClaimsOutstanding - Ints.checkedCast(claims.size());
    }

    private Multimap<String, ByteBuffer> toEventsByChannel(String channel, Collection<ByteBuffer> events) {
        return ImmutableMultimap.<String, ByteBuffer>builder()
                .putAll(channel, events)
                .build();
    }

    private Collection<EventId> toEventIds(Collection<String> eventIds, final String channel) {
        List<EventId> eventIdObjects = Lists.newArrayList();
        for (String eventId : eventIds) {
            eventIdObjects.add(_eventIdSerializer.fromString(eventId, channel));
        }
        return eventIdObjects;
    }

    private EventData toEventData(EventId eventId, ByteBuffer eventData) {
        return new DefaultEventData(_eventIdSerializer.toString(eventId), eventData);
    }

    private Collection<byte[]> toClaimIds(Collection<EventId> eventIds) {
        return Collections2.transform(eventIds, new Function<EventId, byte[]>() {
            @Override
            public byte[] apply(EventId eventId) {
                return eventId.array();
            }
        });
    }

    private void checkClaimTtl(Duration claimTtl) {
        checkArgument(claimTtl.toMillis() >= 0, "ClaimTtl must be >=0");
        checkArgument(claimTtl.toMillis() <= Limits.MAX_CLAIM_TTL.toMillis(), "ClaimTtl must be <=1 hour");
    }

    private void checkLimit(long limit, long max) {
        checkArgument(limit > 0, "Limit must be >0");
        checkArgument(limit <= max, "Limit must be <=%s", max);
    }

    private boolean isDebugLoggingEnabled(String channel) {
        return _log.isDebugEnabled() &&
                !channel.startsWith(SKIP_DEBUG_LOGGING_PREFIX1) &&
                !channel.startsWith(SKIP_DEBUG_LOGGING_PREFIX2);
    }

    private boolean isDebugLoggingEnabled(Multimap<String, ?> eventsByChannel) {
        if (_log.isDebugEnabled()) {
            for (String channel : eventsByChannel.keySet()) {
                if (isDebugLoggingEnabled(channel)) {
                    return true;
                }
            }
        }
        return false;
    }

    private class DaoEventSink implements com.bazaarvoice.emodb.event.db.EventSink {
        private final ClaimSet _claims;
        private final Duration _claimTtl;
        private final String _pollChannel;
        private final EventSink _sink;
        private final int _hardLimit;
        private int _count;
        private boolean _stop;
        private boolean _more;

        DaoEventSink(int hardLimit, EventSink sink) {
            this(null, null, null, hardLimit, sink);
        }

        DaoEventSink(@Nullable String pollChannel, @Nullable ClaimSet claims, @Nullable Duration claimTtl, int hardLimit, EventSink sink) {
            _pollChannel = pollChannel;
            _claims = claims;
            _claimTtl = claimTtl;
            _hardLimit = hardLimit;
            _sink = sink;
        }

        @Override
        public boolean accept(EventId eventId, ByteBuffer eventData) {
            if (_stop || _count >= _hardLimit) {
                _more = true;
                return false;  // Stop looking for events.
            }
            if (claim(_claims, eventId, _claimTtl)) {
                EventSink.Status status = _sink.accept(toEventData(eventId, eventData));
                if (status == EventSink.Status.REJECTED_STOP) {
                    unclaim(_claims, eventId, _claimTtl);
                    markUnread(_pollChannel, Collections.singleton(eventId));
                    _more = true;
                    return false;
                } else if (status == EventSink.Status.ACCEPTED_STOP) {
                    // Go one past what was requested to calculate '_more'.
                    _stop = true;
                }
                _count++;
            }
            return true; // Keep looking for more events.
        }

        int getCount() {
            return _count;
        }

        boolean hasMore() {
            return _more;
        }
    }
}
