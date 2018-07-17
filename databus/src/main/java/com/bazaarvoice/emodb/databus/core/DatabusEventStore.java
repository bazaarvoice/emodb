package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.event.DedupEnabled;
import com.bazaarvoice.emodb.event.api.BaseEventStore;
import com.bazaarvoice.emodb.event.api.DedupEventStore;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.event.api.EventSink;
import com.bazaarvoice.emodb.event.api.EventStore;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The {@link EventStore} used by the {@link @Databus}.
 * <p>
 * Selects the underlying {@link EventStore} implementation based on whether an operation is for an internal system
 * queue or a regular Databus subscription.  This helps ensure we never try to dedup the internal fanout and cross-dc
 * replication queues, which would cause data loss since it would drain their write queues which the readers of
 * those queues don't expect.
 */
public class DatabusEventStore implements BaseEventStore {
    private final EventStore _eventStore;
    private final DedupEventStore _dedupEventStore;
    private final Supplier<Boolean> _dedupEnabled;

    @Inject
    public DatabusEventStore(EventStore eventStore, DedupEventStore dedupEventStore,
                             @DedupEnabled Supplier<Boolean> dedupEnabled) {
        _eventStore = checkNotNull(eventStore, "eventStore");
        _dedupEventStore = checkNotNull(dedupEventStore, "dedupEventStore");
        _dedupEnabled = checkNotNull(dedupEnabled, "dedupEnabled");
    }

    private BaseEventStore select(String subscription) {
        if (_dedupEnabled.get() && !ChannelNames.isNonDeduped(subscription)) {
            return _dedupEventStore;
        } else {
            return _eventStore;
        }
    }

    @Override
    public Iterator<String> listChannels() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(String subscription, ByteBuffer event) {
        _eventStore.add(subscription, event);
    }

    @Override
    public void addAll(String subscription, Collection<ByteBuffer> events) {
        _eventStore.addAll(subscription, events);
    }

    @Override
    public void addAll(Multimap<String, ByteBuffer> eventsByChannel) {
        // For optimal write performance, take advantage of the fact that databus dedup & non-dedup subscriptions
        // use the same back-end write channels, so we can write to both kinds in the same batch.
        _eventStore.addAll(eventsByChannel);
    }

    @Override
    public long getSizeEstimate(String subscription, long limit) {
        return select(subscription).getSizeEstimate(subscription, limit);
    }

    @Override
    public long getClaimCount(String subscription) {
        return select(subscription).getClaimCount(subscription);
    }

    @Override
    public Map<String, Long> snapshotClaimCounts() {
        Map<String, Long> claimCounts = Maps.newLinkedHashMap();
        // The raw event store snapshot claim counts will include write and read channel statistics for
        // deduped queues.  Filter them out.
        for (Map.Entry<String, Long> entry : _eventStore.snapshotClaimCounts().entrySet()) {
            if (ChannelNames.isNonDeduped(entry.getKey())) {
                claimCounts.put(entry.getKey(), entry.getValue());
            }
        }
        // No need to filter the dedup event store claim counts.  They won't conflict with the non-deduped events.
        claimCounts.putAll(_dedupEventStore.snapshotClaimCounts());
        return claimCounts;
    }

    @Override
    public List<EventData> peek(String subscription, int limit) {
        return select(subscription).peek(subscription, limit);
    }

    @Override
    public boolean peek(String subscription, EventSink sink) {
        return select(subscription).peek(subscription, sink);
    }

    @Override
    public List<EventData> poll(String subscription, Duration claimTtl, int limit) {
        return select(subscription).poll(subscription, claimTtl, limit);
    }

    @Override
    public boolean poll(String subscription, Duration claimTtl, EventSink sink) {
        return select(subscription).poll(subscription, claimTtl, sink);
    }

    @Override
    public void renew(String subscription, Collection<String> eventIds, Duration claimTtl, boolean extendOnly) {
        select(subscription).renew(subscription, eventIds, claimTtl, extendOnly);
    }

    @Override
    public void delete(String subscription, Collection<String> eventIds, boolean cancelClaims) {
        select(subscription).delete(subscription, eventIds, cancelClaims);
    }

    @Override
    public void copy(String from, String to, Predicate<ByteBuffer> filter, Date since) {
        // This implements strategies for non-dedup->non-dedup, non-dedup->dedup, dedup->dedup, but not dedup->non-dedup.
        checkArgument(ChannelNames.isNonDeduped(from) || !ChannelNames.isNonDeduped(to),
                "May not copy from regular (deduped) subscription to system (non-deduped) queue: %s -> %s", from, to);
        BaseEventStore eventStore = select(to);
        if (ChannelNames.isNonDeduped(from) && eventStore instanceof DedupEventStore) {
            ((DedupEventStore) eventStore).copyFromRawChannel(from, to, filter, since);
        } else {
            eventStore.copy(from, to, filter, since);
        }
    }

    @Override
    public void move(String from, String to) {
        checkArgument(!ChannelNames.isNonDeduped(from), "May not move from a system (non-deduped) queue: %s -> %s", from, to);
        checkArgument(!ChannelNames.isNonDeduped(to), "May not move to a system (non-deduped) queue: %s -> %s", from, to);
        select(from).move(from, to);
    }

    @Override
    public void unclaimAll(String subscription) {
        select(subscription).unclaimAll(subscription);
    }

    @Override
    public void purge(String subscription) {
        select(subscription).purge(subscription);
    }
}
