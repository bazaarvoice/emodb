package com.bazaarvoice.emodb.event.dedup;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.event.DedupEnabled;
import com.bazaarvoice.emodb.event.api.DedupEventStore;
import com.bazaarvoice.emodb.event.api.DedupEventStoreChannels;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.event.api.EventSink;
import com.bazaarvoice.emodb.event.api.EventStore;
import com.bazaarvoice.emodb.event.api.EventTracer;
import com.bazaarvoice.emodb.event.api.ScanSink;
import com.bazaarvoice.emodb.event.api.SimpleEventSink;
import com.bazaarvoice.emodb.event.core.DefaultEventStore;
import com.bazaarvoice.emodb.event.core.Limits;
import com.bazaarvoice.emodb.event.core.MetricsGroupName;
import com.bazaarvoice.emodb.event.owner.OstrichOwnerFactory;
import com.bazaarvoice.emodb.event.owner.OstrichOwnerGroupFactory;
import com.bazaarvoice.emodb.event.owner.OwnerGroup;
import com.bazaarvoice.emodb.sortedq.api.SortedQueue;
import com.bazaarvoice.emodb.sortedq.api.SortedQueueFactory;
import com.bazaarvoice.emodb.sortedq.core.PersistentSortedQueue;
import com.bazaarvoice.emodb.sortedq.core.ReadOnlyQueueException;
import com.bazaarvoice.emodb.sortedq.db.QueueDAO;
import com.bazaarvoice.ostrich.PartitionContext;
import com.bazaarvoice.ostrich.PartitionContextBuilder;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.ExecutorServiceManager;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An alternative implementation of {@link EventStore} that attempts to sort events and remove duplicates as they
 * stream through the system.  In the case where readers consume events as fast as writers produce them, this should
 * behave roughly equivalently to {@link DefaultEventStore} (albeit somewhat slower due to extra overhead).  But
 * when readers are slower that writers, especially when writers are bursty, the {@code SortedEventStore} will
 * eliminate duplicate events to reduce work and sort events to increase locality of reference.
 * <p/>
 * <p>The implementation combines {@link DefaultEventStore} and {@link PersistentSortedQueue} in the following way:
 * <dl>
 * <dt>Write Channel</dt>
 * <dd>When writers produce events, those events are written to a regular event channel, the "write channel",
 * using the {@link DefaultEventStore}.  This can happen from any machine without special synchronization
 * beyond what {@link DefaultEventStore} already implements.</dd>
 * <p/>
 * <dt>Sorted Queue</dt>
 * <dd>A background thread lazily moves events from the "write channel" to a "sorted queue" implemented using
 * {@link PersistentSortedQueue} which sorts and dedups the events.  The algorithm endeavours to keep the sorted
 * queue full and the write channel empty.  The more data that's in the sorted queue, the higher the chance that
 * duplicates will be identified and eliminated.
 * Because {@code PersistentSortedQueue} is single threaded--it doesn't support concurrent writers from multiple
 * machines--each sorted queue is managed by a single JVM, chosen via ZooKeeper-managed leader election.  The
 * leader election process is managed so that, in normal circumstances, the only server that attempts to win the
 * leader election is the server that would be chosen by the consistent hashing algorithm used by Ostrich to
 * direct Databus and Queue Service clients to the server that manages claims for the queue.  In the steady
 * state, this means that the server that handles {@link #poll} requests for a queue also manages its
 * {@code PersistentSortedQueue}.</dd>
 * <p/>
 * <dt>Read Channel</dt>
 * <dd>When readers read events, they do so using the same {@link #peek}, {@link #poll} and {@link #delete} APIs
 * exposed by {@link DefaultEventStore}.  Internally, the {@code SortedEventStore} moves events from the
 * sorted queue to the read channel only when a client attempts to read events.  Once an event has been
 * moved to the read channel, it stays there until it is acknowledged (via {@link #delete}) and does not
 * get dedup'ed anymore.</dd>
 * </dl>
 * <p/>
 * <p>The general flow of data is from "Write Channel" -> "Sorted Queue" -> "Read Channel".  When readers are fast and
 * drain the sorted queue faster than writers produce new events, the system will optimize the data flow to skip
 * the sorted queue on disk and use "Write Channel" -> (in-memory sort/dedup) -> "Read Channel".
 * <p/>
 * <p>Sometimes read requests arrive at a server that is not the elected manager of the sorted queue.  When this
 * happens, the server will usually make a best-effort attempt to implement the request, subject to the constraints
 * that it may not move data between the write channel, sorted queue or read channel.
 * <ul>
 *     <li>Methods that work well from any server: {@link #addAll}, {@link #getSizeEstimate}</li>
 *     <li>Methods that work from any server, but may return incomplete results or have poor performance
 *     on servers that haven't won the leader election for the sorted queue: {@link #getClaimCount},
 *     {@link #snapshotClaimCounts}, {@link #peek}, {@link #poll}, {@link #renew}, {@link #delete},
 *     {@link #unclaimAll}, {@link #copy}, {@link #copyFromRawChannel}</li>
 *     <li>Methods that will throw {@link ReadOnlyQueueException} on servers that haven't won the leader
 *     election for the sorted queue: {@link #purge}</li>
 * </ul>
 */
public class DefaultDedupEventStore implements DedupEventStore, DedupQueueAdmin {
    /** The amount of time to wait for services to start for fast operations like peek, poll. */
    private static final Duration SERVICE_FAST_WAIT_DURATION = Duration.millis(100);
    /** The amount of time to wait for services to start for slow operations like copy, purge. */
    private static final Duration SERVICE_SLOW_WAIT_DURATION = Duration.standardSeconds(3);
    private static final int COPY_BATCH_SIZE = 2000;

    private final EventStore _delegate;
    private final DedupEventStoreChannels _channels;
    private final QueueDAO _queueDAO;
    private final OwnerGroup<DedupQueue> _ownerGroup;
    private final SortedQueueFactory _sortedQueueFactory;

    @Inject
    public DefaultDedupEventStore(LifeCycleRegistry lifeCycle,
                                  final EventStore delegate,
                                  DedupEventStoreChannels channels,
                                  final QueueDAO queueDAO,
                                  OstrichOwnerGroupFactory ownerGroupFactory,
                                  @DedupEnabled final Supplier<Boolean> dedupEnabled,
                                  @MetricsGroupName String metricsGroup,
                                  final SortedQueueFactory sortedQueueFactory,
                                  final MetricRegistry metricRegistry) {
        _delegate = checkNotNull(delegate, "delegate");
        _channels = checkNotNull(channels, "channels");
        _queueDAO = checkNotNull(queueDAO, "queueDAO");
        _sortedQueueFactory = sortedQueueFactory;

        String name = metricsGroup.substring(metricsGroup.lastIndexOf('.') + 1);

        // Start a background thread for filling sorted queues from write queues.
        String nameFormat = "DedupFill-" + name + "-%d";
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(nameFormat).build();
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(threadFactory);
        lifeCycle.manage(new ExecutorServiceManager(executor, io.dropwizard.util.Duration.seconds(5), nameFormat));

        // Start the queue owner cache that tracks which queues this server is allowed to manage.
        _ownerGroup = lifeCycle.manage(ownerGroupFactory.create(name + "-dedup", new OstrichOwnerFactory<DedupQueue>() {
            @Override
            public PartitionContext getContext(String queue) {
                return PartitionContextBuilder.of(queue);
            }

            @Override
            public DedupQueue create(String queue) {
                String readChannel = _channels.readChannel(queue);
                String writeChannel = _channels.writeChannel(queue);
                return new DedupQueue(queue, readChannel, writeChannel, queueDAO, delegate, dedupEnabled, executor, sortedQueueFactory, metricRegistry);
            }
        }, Duration.standardHours(1)));
    }

    /**
     * Returns the mutable persistent sorted queue if managed by this JVM, {@code null} otherwise.
     */
    @Nullable
    private DedupQueue getQueueReadWrite(String queue, Duration waitDuration) {
        return _ownerGroup.startIfOwner(queue, waitDuration);
    }

    /**
     * Returns the persistent sorted queue managed by this JVM, or a stub that supports only read-only operations if
     * not managed by this JVM.
     */
    private SortedQueue getQueueReadOnly(String queueName, Duration waitDuration) {
        DedupQueue service = getQueueReadWrite(queueName, waitDuration);
        if (service != null) {
            try {
                return service.getQueue();
            } catch (ReadOnlyQueueException e) {
                // Fall through
            }
        }
        return _sortedQueueFactory.create(queueName, true, _queueDAO);
    }

    //
    // DedupQueueAdmin implementation
    //

    @Override
    public Map<String, DedupQueue> getActiveQueues() {
        return _ownerGroup.getServices();
    }

    @Override
    public boolean activateQueue(String queue) {
        return _ownerGroup.startIfOwner(queue, SERVICE_SLOW_WAIT_DURATION) != null;
    }

    //
    // EventStore implementation
    //

    @Override
    public Iterator<String> listChannels() {
        // The implementation of this is unfortunate in that, because the underlying methods return channel
        // and queue names in random order, the only way to dedup names between the 3 underlying data structures
        // is to read all the channels into memory.  Hopefully there aren't so many that this causes problems,
        // but this is why the list channels method shouldn't be exposed as a public supported API, just an
        // internal feature that can be re-implemented or reconceived if necessary.
        Set<String> queues = Sets.newHashSet();

        // Enumerate the persistent sorted queues
        Iterators.addAll(queues, _queueDAO.listQueues());

        // Enumerate the read & write channels.
        Iterator<String> channelIter = _delegate.listChannels();
        while (channelIter.hasNext()) {
            String channel = channelIter.next();
            String queue;
            if ((queue = _channels.queueFromReadChannel(channel)) != null) {
                queues.add(queue);
            } else if ((queue = _channels.queueFromWriteChannel(channel)) != null) {
                queues.add(queue);
            }
        }

        return queues.iterator();  // Unordered to be consistent with non-dedup'd event store
    }

    @Override
    public void add(String queue, ByteBuffer event) {
        checkNotNull(queue, "queue");
        checkNotNull(event, "event");

        _delegate.add(_channels.writeChannel(queue), event);
    }

    @Override
    public void addAll(String queue, Collection<ByteBuffer> events) {
        checkNotNull(queue, "queue");
        checkNotNull(events, "events");

        _delegate.addAll(_channels.writeChannel(queue), events);
    }

    @Override
    public void addAll(Multimap<String, ByteBuffer> eventsByQueue) {
        checkNotNull(eventsByQueue, "eventsByQueue");

        Multimap<String, ByteBuffer> eventsByWriteChannel = ArrayListMultimap.create();
        for (Map.Entry<String, Collection<ByteBuffer>> entry : eventsByQueue.asMap().entrySet()) {
            eventsByWriteChannel.putAll(_channels.writeChannel(entry.getKey()), entry.getValue());
        }
        _delegate.addAll(eventsByWriteChannel);
    }

    @Override
    public long getSizeEstimate(String queue, long limit) {
        checkNotNull(queue, "queue");
        checkLimit(limit, Long.MAX_VALUE);

        return _delegate.getSizeEstimate(_channels.writeChannel(queue), limit) +
                getQueueReadOnly(queue, SERVICE_FAST_WAIT_DURATION).sizeEstimate() +
                _delegate.getSizeEstimate(_channels.readChannel(queue), limit);
    }

    @Override
    public long getClaimCount(String queue) {
        checkNotNull(queue, "queue");

        // Ignore write channel claims.  Client poll() requests only claim on the read channel.
        return _delegate.getClaimCount(_channels.readChannel(queue));
    }

    @Override
    public Map<String, Long> snapshotClaimCounts() {
        Map<String, Long> countsByReadChannel = _delegate.snapshotClaimCounts();
        Map<String, Long> countsByQueue = Maps.newLinkedHashMap();
        for (Map.Entry<String, Long> entry : countsByReadChannel.entrySet()) {
            String queue = _channels.queueFromReadChannel(entry.getKey());
            if (queue == null) {
                continue;  // Ignore write channel claims.  Client poll() requests only claim on the read channel.
            }
            countsByQueue.put(queue, entry.getValue());
        }
        return countsByQueue;
    }

    @Override
    public List<EventData> peek(String channel, int limit) {
        SimpleEventSink sink = new SimpleEventSink(limit);
        peek(channel, sink);
        return sink.getEvents();
    }

    @Override
    public boolean peek(String queue, EventSink sink) {
        checkNotNull(queue, "queue");
        checkLimit(sink.remaining(), Limits.MAX_PEEK_LIMIT);

        // Ideally, peek() would have no side-effects.  Unfortunately, it must return EventData objects that include
        // read channel event IDs, so peek can only return events in the read channel.  Therefore, for peek to be
        // useful, it must copy events to the read channel when the read channel has fewer than 'limit' events.  This
        // means that the full peek() implementation must run on the server that owns the queue so it can have read/
        // write access to the PersistentSortedQueue data structure.

        DedupQueue service = getQueueReadWrite(queue, SERVICE_FAST_WAIT_DURATION);
        if (service != null) {
            try {
                return service.peek(sink);
            } catch (ReadOnlyQueueException e) {
                // Raced w/losing leadership and lost.  Fall through.
            }
        }

        // If this server doesn't own the queue, we can't interact with it.  The best we can do is read
        // directly from the read channel which doesn't require cross-server synchronization.
        return _delegate.peek(_channels.readChannel(queue), sink);
    }

    @Override
    public List<EventData> poll(String channel, Duration claimTtl, int limit) {
        SimpleEventSink sink = new SimpleEventSink(limit);
        poll(channel, claimTtl, sink);
        return sink.getEvents();
    }

    @Override
    public boolean poll(String queue, Duration claimTtl, EventSink sink) {
        checkNotNull(queue, "queue");
        checkLimit(sink.remaining(), Limits.MAX_POLL_LIMIT);

        DedupQueue service = getQueueReadWrite(queue, SERVICE_FAST_WAIT_DURATION);
        if (service != null) {
            try {
                return service.poll(claimTtl, sink);
            } catch (ReadOnlyQueueException e) {
                // Raced w/losing leadership and lost.  Fall through.
            }
        }

        // If this server doesn't own the queue, we can't interact with it.  We aren't allowed to copy events
        // from the write channel to the read channel because then they'd bypass the sorted queue.  The best we can
        // do is read directly from the read channel which doesn't require cross-server synchronization, although
        // it's likely to lead to duplicate events.
        return _delegate.poll(_channels.readChannel(queue), claimTtl, sink);
    }

    @Override
    public void renew(String queue, Collection<String> eventIds, Duration claimTtl, boolean extendOnly) {
        checkNotNull(queue, "queue");

        _delegate.renew(_channels.readChannel(queue), eventIds, claimTtl, extendOnly);
    }

    @Override
    public void delete(String queue, Collection<String> eventIds, boolean cancelClaims) {
        checkNotNull(queue, "queue");

        DedupQueue service = getQueueReadWrite(queue, SERVICE_FAST_WAIT_DURATION);
        if (service != null) {
            service.delete(eventIds, cancelClaims);
        } else {
            _delegate.delete(_channels.readChannel(queue), eventIds, cancelClaims);
        }
    }

    @Override
    public void unclaimAll(String queue) {
        checkNotNull(queue, "queue");

        _delegate.unclaimAll(_channels.readChannel(queue));
    }

    /**
     * Copies events matching the specified predicate from one dedup queue to another.
     * <p>
     * Note: this method expects both "from" and "to" are dedup queues.  If "from" queue is not, use
     * {@link #copyFromRawChannel} instead to avoid starting a DedupQueueService for "from" that will
     * drain it and move its data to a sorted queue.
     */
    @Override
    public void copy(String from, String to, Predicate<ByteBuffer> filter, Date since, @Nullable EventTracer tracer) {
        checkNotNull(from, "from");
        checkNotNull(to, "to");

        ScanSink sink = newCopySink(to, tracer, "DefaultDedupEventStore#copy");

        _delegate.scan(_channels.writeChannel(from), filter, sink, COPY_BATCH_SIZE, since);

        SortedQueue source = getQueueReadOnly(from, SERVICE_SLOW_WAIT_DURATION);
        Iterator<List<ByteBuffer>> it = Iterators.partition(source.scan(null, Long.MAX_VALUE), COPY_BATCH_SIZE);
        while (it.hasNext()) {
            List<ByteBuffer> events = it.next();
            sink.accept(ImmutableList.copyOf(Iterables.filter(events, filter)));  // Copy so filter is evaluated only once per record.
        }

        _delegate.scan(_channels.readChannel(from), filter, sink, COPY_BATCH_SIZE, since);
    }

    @Override
    public void copyFromRawChannel(String from, String to, Predicate<ByteBuffer> filter, Date since, @Nullable EventTracer tracer) {
        checkNotNull(from, "from");
        checkNotNull(to, "to");

        _delegate.scan(from, filter, newCopySink(to, tracer, "DefaultDedupEventStore#copyFromRawChannel"), COPY_BATCH_SIZE, since);
    }

    private ScanSink newCopySink(final String to, final EventTracer tracer, final String tracerContext) {
        // Copy to one of 2 destinations: "to" write channel, sorted queue.  Either would be correct.
        //
        // This operation runs most efficiently when this server owns the "to" queue.  This avoids an extra copy later
        // of the data from the write channel to the sorted queue, and should account for the bulk of the copy since
        // most the data should reside long-term in the sorted queue.
        //
        // Since this method is run asynchronously as part of a job raise a ReadOnlyQueueException to force the
        // job to run on the server which initially owns the queue.  However, since copy can take a while, we should
        // handle the situation where a server roll in the middle of the copy causes this server to lose ownership of
        // the sorted queue and fall back to copying to the write channel instead.

        final DedupQueue toQueue = getQueueReadWrite(to, SERVICE_SLOW_WAIT_DURATION);
        if (toQueue == null) {
            throw new ReadOnlyQueueException("Cannot copy to server that does not own the queue: " + to);
        }

        return new ScanSink() {
            private DedupQueue _toQueue = toQueue;
            private final String _toChannel = _channels.writeChannel(to);
            @Override
            public void accept(List<ByteBuffer> events) {
                if (tracer != null) {
                    events.forEach(event -> tracer.trace(tracerContext, event));
                }

                // Try to write to the dedup queue.
                if (_toQueue != null) {
                    try {
                        _toQueue.addAll(events);
                    } catch (ReadOnlyQueueException e) {
                        // We've lost the leadership lock on the persistent sorted queue.
                        _toQueue = null;
                    }
                }
                // If the dedup queue isn't available, fall back to the write channel.
                if (_toQueue == null) {
                    _delegate.addAll(_toChannel, events);
                }
            }
        };
    }

    @Override
    public void move(String from, String to, @Nullable EventTracer tracer) {
        checkNotNull(from, "from");
        checkNotNull(to, "to");

        moveToRawChannel(from, _channels.writeChannel(to), tracer);
    }

    @Override
    public void moveToRawChannel(String from, String to, @Nullable EventTracer tracer) {
        checkNotNull(from, "from");
        checkNotNull(to, "to");

        DedupQueue source = getQueueReadWrite(from, SERVICE_SLOW_WAIT_DURATION);
        if (source == null) {
            throw new ReadOnlyQueueException("Cannot move from server that does not own the source queue: " + from);
        }
        source.moveToRawChannel(to, tracer);
    }

    @Override
    public void purge(String queue) {
        checkNotNull(queue, "queue");

        DedupQueue service = getQueueReadWrite(queue, SERVICE_SLOW_WAIT_DURATION);
        if (service == null) {
            throw new ReadOnlyQueueException("Cannot purge from server that does not own the queue: " + queue);
        }
        service.purge();
    }

    private void checkLimit(long limit, long max) {
        checkArgument(limit > 0, "Limit must be >0");
        checkArgument(limit <= max, "Limit must be <=%s", max);
    }
}
