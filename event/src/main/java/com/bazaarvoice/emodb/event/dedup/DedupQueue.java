package com.bazaarvoice.emodb.event.dedup;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.ServiceFailureListener;
import com.bazaarvoice.emodb.event.api.DedupEventStore;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.event.api.EventSink;
import com.bazaarvoice.emodb.event.api.EventStore;
import com.bazaarvoice.emodb.event.api.SimpleEventSink;
import com.bazaarvoice.emodb.event.core.Limits;
import com.bazaarvoice.emodb.sortedq.api.Consumer;
import com.bazaarvoice.emodb.sortedq.api.SortedQueue;
import com.bazaarvoice.emodb.sortedq.api.SortedQueueFactory;
import com.bazaarvoice.emodb.sortedq.core.ByteBufferOrdering;
import com.bazaarvoice.emodb.sortedq.core.ReadOnlyQueueException;
import com.bazaarvoice.emodb.sortedq.db.QueueDAO;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/** In-memory state for a single {@link DefaultDedupEventStore} sorted queue. */
public class DedupQueue extends AbstractIdleService {
    private static final Logger _log = LoggerFactory.getLogger(DedupQueue.class);

    private static final Duration LAZY_FILL_DELAY = Duration.standardSeconds(1);
    private static final Duration SORTED_QUEUE_TIMEOUT = Duration.millis(100);
    private static final ByteBufferOrdering ORDERING = ByteBufferOrdering.INSTANCE;

    private final String _name;
    private final QueueDAO _queueDAO;
    private final ScheduledExecutorService _executor;
    private final EventStore _eventStore;
    private final Supplier<Boolean> _dedupEnabled;
    private final String _readChannel;
    private final String _writeChannel;
    private final AsyncFiller _asyncFiller = new AsyncFiller();
    private volatile SortedQueue _queue;
    private final SortedQueueFactory _sortedQueueFactory;

    public DedupQueue(String name, String readChannel, String writeChannel,
                      QueueDAO queueDAO, EventStore eventStore, Supplier<Boolean> dedupEnabled,
                      ScheduledExecutorService executor, SortedQueueFactory sortedQueueFactory, MetricRegistry metricRegistry) {
        _name = checkNotNull(name, "name");
        _readChannel = checkNotNull(readChannel, "readChannel");
        _writeChannel = checkNotNull(writeChannel, "writeChannel");
        _queueDAO = checkNotNull(queueDAO, "queueDAO");
        _eventStore = checkNotNull(eventStore, "eventStore");
        _dedupEnabled = checkNotNull(dedupEnabled, "dedupEnabled");
        _executor = checkNotNull(executor, "executor");
        _sortedQueueFactory = sortedQueueFactory;
        ServiceFailureListener.listenTo(this, metricRegistry);
    }

    @Override
    protected void startUp() throws Exception {
        _queue = _sortedQueueFactory.create(_name, _queueDAO);
        _asyncFiller.start();
    }

    @Override
    protected void shutDown() throws Exception {
        if (_queue != null) {
            _queue.setReadOnly();  // Lost leadership, prevent further writes to the database.
            _queue = null;
        }
    }

    public SortedQueue getQueue() throws ReadOnlyQueueException {
        SortedQueue queue = _queue;
        if (queue == null) {
            throw new ReadOnlyQueueException();  // Service is no longer running.
        }
        return queue;
    }

    /** Implementation of {@link EventStore#addAll(String, Collection)}. */
    public void addAll(Collection<ByteBuffer> records) {
        getQueue().addAll(records);
    }

    /** Implementation of {@link EventStore#peek(String, EventSink)}. */
    public boolean peek(EventSink sink) {
        return peekOrPoll(null, sink);
    }

    /** Implementation of {@link EventStore#poll(String, Duration, EventSink)}. */
    public boolean poll(Duration claimTtl, EventSink sink) {
        return peekOrPoll(claimTtl, sink);
    }

    /** Implementation of {@link EventStore#delete(String, Collection, boolean)}. */
    public void delete(Collection<String> eventIds, boolean cancelClaims) {
        _eventStore.delete(_readChannel, eventIds, cancelClaims);
    }

    /** Implementation of {@link DedupEventStore#moveToRawChannel(String, String)}. */
    public void moveToRawChannel(final String toChannel) {
        // Pause the asynchronous filler; it is not thread safe to fill while the move is taking place.  This is
        // because the move process may reassign slabs from this queue's write channel to toChannel's write channel. If the filler
        // is running it will poll the write channel (which is safe) and then delete the events it polled after they
        // are written to the read channel (which is not safe).  This is not thread safe because events are deleted
        // based on the containing slab, and if the slab has been moved to toChannel then this has the effect of
        // deleting the events from toChannel as well.
        _asyncFiller.pause();
        try {
            SortedQueue queue = getQueue();
            _eventStore.move(_writeChannel, toChannel);
            queue.drainTo(new Consumer() {
                @Override
                public void consume(List<ByteBuffer> records) {
                    _eventStore.addAll(toChannel, records);
                }
            }, Long.MAX_VALUE);
            _eventStore.move(_readChannel, toChannel);
        } finally {
            _asyncFiller.resume();
        }
    }

    /** Implementation of {@link EventStore#purge(String)}. */
    public void purge() {
        SortedQueue queue = getQueue();
        _eventStore.purge(_writeChannel);
        queue.clear();
        _eventStore.purge(_readChannel);
    }

    //
    // Peek & Poll Implementation
    //

    /** Implements peek() or poll() based on whether claimTtl is null or non-null. */
    private boolean peekOrPoll(@Nullable Duration claimTtl, EventSink rawSink) {
        // If dedup activity is disabled then we're in the process of falling back to regular non-dedup'd channels.
        // Don't do anything that might move events around--we're likely racing a call to moveToRawChannel().
        if (!_dedupEnabled.get()) {
            return false;
        }

        // When polling, protect from bad pollers that never ack.
        if (claimTtl != null && _eventStore.getClaimCount(_readChannel) >= Limits.MAX_CLAIMS_OUTSTANDING) {
            return false;
        }

        TrackingEventSink sink = new TrackingEventSink(rawSink);

        // There are three places in which to look for events:
        // 1. Read channel
        // 2. Persistent sorted queue
        // 3. Write channel
        // This code is designed to, in the *common* case, poll only one of those sources.  As things transition
        // between the following 3 phases it will occasionally poll multiple sources, but hopefully a server will
        // spend most of its time in one particular phase, resulting in efficient polling overall.
        // Phase 1: Try to keep the read channel empty.  Start by draining the read channel of un-acked events.
        // Phase 2: When readers are slower than writers, events build up in the sorted queue.  Source from the sorted queue.
        // Phase 3: When readers are faster than writers, the sorted queue isn't useful.  Source directly from the write channel.

        // The read channel only contains unclaimed events when (a) claims have expired because something has gone
        // wrong (crash, restart, timeout, etc.) or (b) polling the sorted queue or write channel pulled more items
        // than the sink would accept and we had to write the overflow to the read channel.  Neither is the common
        // case, so rely on the DefaultEventStore "empty channel" cache to rate limit actual reads to one-per-second.
        boolean moreRead = peekOrPollReadChannel(claimTtl, sink);
        if (sink.isDone()) {
            return moreRead;
        }

        // Do NOT dedup events in-memory between the read channel and the other sources.  Once an event makes it to
        // the read channel we can't dedup it with anything else.  That lets us support the following sequence:
        // 1. process A adds EventX
        // 2. process B polls EventX and begins working on it
        // 3. process A makes a change and adds EventX to be re-evaluated
        // 4. process B acks EventX
        // The event added in step 3 must be kept separate from the event added in step 1 so it doesn't get acked
        // in step 4.  Essentially, once a poller starts working on an event (indicated by its presence in the read
        // channel) we can't dedup/consolidate that event with any other events.
        Set<ByteBuffer> unique = Sets.newHashSet();

        // Search for events in the write channel, copying them to the sorted queue or, under certain circumstances,
        // copying them directly to the read channel before returning.
        boolean moreWrite = peekOrPollWriteChannel(claimTtl, sink, unique);
        if (moreWrite) {
            // There are more unconsumed write channel events.  Move them asynchronously to the sorted queue.
            _asyncFiller.start();
        }

        // Search for events in the sorted queue, copying them to the read channel before returning.
        boolean moreSorted = peekOrPollSortedQueue(claimTtl, sink, unique);

        return moreWrite || moreSorted;
    }

    private boolean peekOrPollReadChannel(@Nullable Duration claimTtl, TrackingEventSink sink) {
        if (claimTtl == null) {
            return _eventStore.peek(_readChannel, sink);
        } else {
            return _eventStore.poll(_readChannel, claimTtl, sink);
        }
    }

    private boolean addAndPeekOrPollReadChannel(Collection<ByteBuffer> records, @Nullable Duration claimTtl, TrackingEventSink sink) {
        if (records.isEmpty()) {
            return false;
        }
        if (claimTtl == null) {
            return _eventStore.addAllAndPeek(_readChannel, records, sink);
        } else {
            return _eventStore.addAllAndPoll(_readChannel, records, claimTtl, sink);
        }
    }

    private boolean peekOrPollWriteChannel(@Nullable final Duration claimTtl, final TrackingEventSink sink, final Set<ByteBuffer> unique) {
        // Move records from the write channel to the read channel, deduping entries in-memory.
        // Always poll at least once to ensure that copying from write->sorted doesn't fall behind readers.
        // The EventStore "empty channel" cache will no-op the poll when it's likely the write channel has no data.
        Drained drained = drainWriteChannelTo(new Consumer() {
            // The consume() method will be called zero or one time.
            @Override
            public void consume(List<ByteBuffer> records) {
                // If we see that sorted queue readers might be getting way ahead of filling from the write
                // channel, throttle the readers.  Make the readers do some of the filling and limit them to
                // consuming at most 50% of the records they drain from the write channel.
                if (records.size() == Limits.MAX_POLL_LIMIT) {
                    sink.setHardLimit(Limits.MAX_POLL_LIMIT / 2);
                }

                // Eliminate duplicates in-memory.
                records = filterDuplicates(records, unique);

                // If the sorted queue is empty, we can dedup entirely in memory and transfer records directly
                // to the read channel, bypassing the sorted queue completely.
                if (!records.isEmpty() && !sink.isDone() && getQueue().isEmpty()) {
                    records = sorted(records);  // Make the results pretty.
                    // We really don't want to copy events to the read channel unless the sink takes them, so start
                    // by passing the sink exactly the number of events it says it needs via sink.remaining().
                    int padding = 0;
                    do {
                        int count = Math.min(sink.remaining() + padding, records.size());
                        addAndPeekOrPollReadChannel(records.subList(0, count), claimTtl, sink);
                        records = records.subList(count, records.size());
                        padding += 10;
                    } while (!records.isEmpty() && !sink.isDone());
                }

                // Move the remaining records to the sorted queue.
                if (!records.isEmpty()) {
                    getQueue().addAll(records);
                    unique.removeAll(Sets.newHashSet(records)); // Remove records from "unique" since we didn't give them to the sink.
                }
            }
        }, Limits.MAX_POLL_LIMIT);
        return drained == Drained.SOME;
    }

    private boolean peekOrPollSortedQueue(@Nullable final Duration claimTtl, final TrackingEventSink sink, final Set<ByteBuffer> unique) {
        // Move records from the sorted queue to the read channel.
        SortedQueue queue = getQueue();
        int padding = 0;
        long stopAt = System.currentTimeMillis() + SORTED_QUEUE_TIMEOUT.getMillis();  // Don't loop forever
        long remainingTime;
        // As events are drained from the sorted queue to the read channel keep track of whether there were more events
        // than were accepted by the sink.
        AtomicBoolean moreEventsInReadChannel = new AtomicBoolean(false);
        while (!sink.isDone() && !queue.isEmpty() && (remainingTime = stopAt - System.currentTimeMillis()) > 0) {
            queue.drainTo(new Consumer() {
                @Override
                public void consume(List<ByteBuffer> records) {
                    // The records will be deleted from the PersistentSortedQueue when this method returns, so to avoid
                    // possible data loss we *must* copy them to persistent storage (ie. read channel) before returning.
                    boolean more = addAndPeekOrPollReadChannel(filterDuplicates(records, unique), claimTtl, sink);
                    moreEventsInReadChannel.set(more);
                }
            }, sink.remaining() + padding, Duration.millis(remainingTime));
            // Increase the padding each time in case the sink finds lots of events it can consolidate.
            padding = Math.min(padding + 10, 1000);
        }
        return !queue.isEmpty() || moreEventsInReadChannel.get();
    }

    private enum Drained {
        NONE, SOME, ALL,
    }

    private Drained drainWriteChannelTo(Consumer consumer, int limit) {
        // Because we're trying to move records from the write channel to the read channel, use "poll" with the
        // write channel even if we're trying to "peek" the dedup queue.
        SimpleEventSink simpleSink = new SimpleEventSink(limit);
        boolean more = _eventStore.poll(_writeChannel, Duration.standardSeconds(30), simpleSink);
        List<EventData> events = simpleSink.getEvents();
        if (events.isEmpty()) {
            return Drained.NONE;
        }
        consumer.consume(getEventData(events));
        // Once the records are in the read channel we are safe to remove them from the write channel.
        _eventStore.delete(_writeChannel, getEventIds(events), true);
        return more ? Drained.SOME : Drained.ALL;
    }

    private List<ByteBuffer> filterDuplicates(Collection<ByteBuffer> records, Set<ByteBuffer> unique) {
        List<ByteBuffer> results = Lists.newArrayListWithCapacity(records.size());
        for (ByteBuffer record : records) {
            if (unique.add(record)) {
                results.add(record);
            }
        }
        return results;
    }

    private List<ByteBuffer> sorted(Iterable<ByteBuffer> events) {
        return ORDERING.immutableSortedCopy(events);
    }

    private List<String> getEventIds(List<EventData> events) {
        return Lists.transform(events, new Function<EventData, String>() {
            @Override
            public String apply(EventData event) {
                return event.getId();
            }
        });
    }

    private List<ByteBuffer> getEventData(List<EventData> events) {
        return Lists.transform(events, new Function<EventData, ByteBuffer>() {
            @Override
            public ByteBuffer apply(EventData event) {
                return event.getData();
            }
        });
    }

    // Used by the DedupQueueTask to expose dedup queue metrics, for debugging.
    @Override
    public String toString() {
        SortedQueue queue = _queue;
        Objects.ToStringHelper helper = Objects.toStringHelper(this);
        helper.add("name", _name);
        helper.add("#write", _eventStore.getSizeEstimate(_writeChannel, 1000));
        helper.add("#write-claims", _eventStore.getClaimCount(_writeChannel));
        helper.add("#dedup", (queue != null) ? queue.sizeEstimate() : "n/a");
        helper.add("#read", _eventStore.getSizeEstimate(_readChannel, 1000));
        helper.add("#read-claims", _eventStore.getClaimCount(_readChannel));
        if (_asyncFiller.isFilling()) {
            helper.addValue("FILLING");
            helper.add("idle", _asyncFiller.getConsecutiveNoops());
        }
        return helper.toString();
    }

    /** Does dedup work in the background by filling the SortedQueue from the write channel. */
    private class AsyncFiller implements Runnable {
        private volatile ScheduledFuture<?> _fillFuture;
        private volatile boolean _paused;
        private volatile int _consecutiveNoops;

        boolean isFilling() {
            return _fillFuture != null;
        }

        int getConsecutiveNoops() {
            return _consecutiveNoops;
        }

        void start() {
            schedule(Duration.ZERO, false);
        }

        private synchronized void schedule(@Nullable Duration delay, boolean fromFillerThread) {
            if (_paused) {
                // Ignore scheduling any future if the filler is paused
                return;
            }
            // Clear out the old future.
            if (fromFillerThread) {
                _fillFuture = null;
            }
            // Schedule the new future.
            if (_fillFuture == null && delay != null) {
                _fillFuture = _executor.schedule(this, delay.getMillis(), TimeUnit.MILLISECONDS);
            }
        }

        @Override
        public void run() {
            Duration nextFill = null;
            try {
                nextFill = fill();
            } catch (ReadOnlyQueueException e) {
                // Lost leadership.  No big deal, ignore the exception.
            } catch (Throwable t) {
                _log.error("Unexpected exception filling queue: {}", _name, t);
            } finally {
                schedule(nextFill, true);
            }
        }

        /**
         * Pauses this instance from asynchronously filling the dedup queue.  If a fill iteration is currently in
         * progress this call blocks until it is complete.
         */
        public void pause() {
            Future<?> fillFuture;
            synchronized (this) {
                if (_paused && _fillFuture == null) {
                    // Already paused
                    return;
                }

                _paused = true;

                fillFuture = _fillFuture;
            }

            if (fillFuture != null) {
                // A fill is scheduled or running so the pause must block until it is complete.  This also has to be
                // done while not synchronized otherwise we could deadlock if the fill thread calls schedule().
                try {
                    fillFuture.get();
                } catch (Throwable t) {
                    // Should never happen since all Throwables are caught in run()
                    _log.error("Unexpected exception waiting for queue pause to complete: {}", _name, t);
                }

                synchronized (this) {
                    // Protecting against the extremely unlikely situation where pause() is called multiple times
                    // concurrently prior to resume().
                    if (fillFuture == _fillFuture) {
                        _fillFuture = null;
                    }
                }
            }
        }

        /**
         * Lifts the pause on asynchronous filling from a prior call to {@link #pause()}.  Note that this does not
         * immediately resume the fill; it only allows the fill to start again if and when {@link #start()} is called.
         */
        public synchronized void resume() {
            _paused = false;
        }

        /**
         * Copies a batch of events from the write queue to the sorted queue.  This is always invoked via the
         * scheduled executor.
         */
        private Duration fill() {
            State state = state();
            if (state != State.STARTING && state != State.RUNNING) {
                return null;
            }

            // If dedup activity is disabled then don't move anything out of the write channel.
            if (!_dedupEnabled.get()) {
                return null;
            }

            Drained drained = drainWriteChannelTo(new Consumer() {
                @Override
                public void consume(List<ByteBuffer> records) {
                    getQueue().addAll(records);
                }
            }, Limits.MAX_POLL_LIMIT);

            Duration nextFill;
            if (drained != Drained.NONE) {
                // If there are more events to fetch, schedule another fill immediately.  Otherwise wait a while.
                _consecutiveNoops = 0;
                boolean more = (drained == Drained.SOME);
                nextFill = more ? Duration.ZERO : LAZY_FILL_DELAY;
            } else if (++_consecutiveNoops < 3) {
                // We didn't find anything.  Try again in a little while.
                nextFill = LAZY_FILL_DELAY;
            } else {
                // The last few polls didn't find anything.  Stop polling for now.
                _consecutiveNoops = 0;
                nextFill = null;
            }
            return nextFill;
        }
    }

    /** Counts known event counts and tracks when the delegate sink returns a stop status. */
    private static class TrackingEventSink implements EventSink {
        private final EventSink _delegate;
        private int _hardLimit = Limits.MAX_PEEK_LIMIT;
        private int _count;
        private boolean _done;

        TrackingEventSink(EventSink delegate) {
            _delegate = delegate;
        }

        @Override
        public int remaining() {
            int remaining = Math.min(_delegate.remaining(), _hardLimit - _count);
            // Remaining should always be >0 until accept() returns STOP, after which DedupQueue should not call
            // remaining() again.  It's for client EventSink implementations to return status values from accept
            // that are inconsistent with remaining(), so it's best to not trust remaining() after STOP.
            checkState(!_done && remaining > 0);
            return remaining;
        }

        @Override
        public Status accept(EventData event) {
            Status status = _delegate.accept(event);

            // Update tracking variables.
            if (status == Status.ACCEPTED_CONTINUE && ++_count >= _hardLimit) {
                status = Status.ACCEPTED_STOP;
            }
            if (status == Status.ACCEPTED_STOP || status == Status.REJECTED_STOP) {
                _done = true;
            }
            return status;
        }

        void setHardLimit(int hardLimit) {
            _hardLimit = hardLimit;
            if (_count >= _hardLimit) {
                _done = true;
            }
        }

        boolean isDone() {
            return _done;
        }
    }
}
