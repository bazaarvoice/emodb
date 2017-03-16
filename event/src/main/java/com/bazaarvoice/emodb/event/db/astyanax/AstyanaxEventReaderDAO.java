package com.bazaarvoice.emodb.event.db.astyanax;

import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.metrics.InstrumentedCache;
import com.bazaarvoice.emodb.common.dropwizard.metrics.ParameterizedTimed;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.event.core.MetricsGroupName;
import com.bazaarvoice.emodb.event.db.EventId;
import com.bazaarvoice.emodb.event.db.EventReaderDAO;
import com.bazaarvoice.emodb.event.db.EventSink;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.netflix.astyanax.Execution;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.TimeUUIDSerializer;
import com.netflix.astyanax.serializers.UUIDSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import io.dropwizard.lifecycle.ExecutorServiceManager;
import io.dropwizard.util.Duration;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class AstyanaxEventReaderDAO implements EventReaderDAO {
    private static final Logger _log = LoggerFactory.getLogger(AstyanaxEventReaderDAO.class);
    private static final DateTimeFormatter ISO_FORMATTER = ISODateTimeFormat.dateTime().withZoneUTC();

    private static final int NUM_CLEANUP_THREADS = 2;
    private static final int MAX_CLEANUP_QUEUE_LENGTH = 100;
    private static final int SLAB_MOVE_BATCH = 100;

    private final CassandraKeyspace _keyspace;
    private final ManifestPersister _manifestPersister;
    private final ExecutorService _cleanupExecutor;
    private final LoadingCache<ChannelSlab, SlabCursor> _openSlabCursors;
    private final LoadingCache<ChannelSlab, SlabCursor> _closedSlabCursors;
    private final Meter _staleSlabMeter;

    @Inject
    public AstyanaxEventReaderDAO(LifeCycleRegistry lifeCycle,
                                  CassandraKeyspace keyspace,
                                  ManifestPersister manifestPersister,
                                  @MetricsGroupName String metricsGroup,
                                  MetricRegistry metricRegistry) {
        this(keyspace, manifestPersister, metricsGroup, defaultCleanupExecutor(metricsGroup, lifeCycle, metricRegistry), metricRegistry);
    }

    @VisibleForTesting
    AstyanaxEventReaderDAO(CassandraKeyspace keyspace,
                           ManifestPersister manifestPersister,
                           String metricsGroup,
                           ExecutorService cleanupExecutor,
                           MetricRegistry metricRegistry) {
        _keyspace = keyspace;
        _manifestPersister = manifestPersister;
        _cleanupExecutor = cleanupExecutor;

        CacheLoader<ChannelSlab, SlabCursor> slabCursorFactory = new CacheLoader<ChannelSlab, SlabCursor>() {
            @Override
            public SlabCursor load(ChannelSlab key) {
                return new SlabCursor();
            }
        };
        // Don't rewind an open slab more often than once every 250ms, closed slab every 10 seconds.
        _openSlabCursors = CacheBuilder.newBuilder().
                expireAfterWrite(250, TimeUnit.MILLISECONDS).
                maximumSize(10000).   // 10k - relatively small since the consequences of a miss are minor.
                recordStats().
                build(slabCursorFactory);
        _closedSlabCursors = CacheBuilder.newBuilder().
                expireAfterWrite(10, TimeUnit.SECONDS).
                maximumSize(100000).  // 100k - bigger than the open slab cache since it's more important for performance.
                recordStats().
                build(slabCursorFactory);
        InstrumentedCache.instrument(_openSlabCursors, metricRegistry, metricsGroup, "openSlabCursors", false);
        InstrumentedCache.instrument(_closedSlabCursors, metricRegistry, metricsGroup, "closedSlabCursors", false);

        _staleSlabMeter = metricRegistry.meter(MetricRegistry.name(metricsGroup, "AstyanaxEventReaderDAO", "stale_slabs"));
    }

    private static ExecutorService defaultCleanupExecutor(String metricsGroup, LifeCycleRegistry lifeCycle, MetricRegistry metricRegistry) {
        final Meter meter = metricRegistry.meter(MetricRegistry.name(metricsGroup, "AstyanaxEventReaderDAO", "discarded_slab_cleanup"));
        String nameFormat = "Events Slab Reader Cleanup-" + metricsGroup.substring(metricsGroup.lastIndexOf('.') + 1) + "-%d";
        ExecutorService executor = new ThreadPoolExecutor(
                NUM_CLEANUP_THREADS, NUM_CLEANUP_THREADS,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(MAX_CLEANUP_QUEUE_LENGTH),
                new ThreadFactoryBuilder().setNameFormat(nameFormat).build(),
                new ThreadPoolExecutor.DiscardPolicy() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                        meter.mark();
                    }
                });
        lifeCycle.manage(new ExecutorServiceManager(executor, Duration.seconds(5), nameFormat));
        return executor;
    }

    @Override
    public Iterator<String> listChannels() {
        final Iterator<Row<String, ByteBuffer>> rowIter = execute(
                _keyspace.prepareQuery(ColumnFamilies.MANIFEST, ConsistencyLevel.CL_LOCAL_QUORUM)
                        .getAllRows()
                        .setRowLimit(1000)
                        .withColumnRange(new RangeBuilder().setLimit(1).build()))
                .iterator();
        return new AbstractIterator<String>() {
            @Override
            protected String computeNext() {
                while (rowIter.hasNext()) {
                    Row<String, ByteBuffer> row = rowIter.next();
                    if (!row.getColumns().isEmpty()) {
                        return row.getKey();
                    }
                }
                return endOfData();
            }
        };
    }

    @ParameterizedTimed(type = "AstyanaxEventReaderDAO")
    @Override
    public long count(String channel, long limit) {
        long total = 0;

        // Note: unlike the read methods, the count method does not delete empty slabs (!open && count==0) since
        // we can't trust results w/ConsistencyLevel.CL_ONE.

        Iterable<Column<ByteBuffer>> manifestColumns = executePaginated(
                _keyspace.prepareQuery(ColumnFamilies.MANIFEST, ConsistencyLevel.CL_ONE)
                        .getKey(channel)
                        .withColumnRange(new RangeBuilder().setLimit(100).build())
                        .autoPaginate(true));

        for (Column<ByteBuffer> manifestColumn : manifestColumns) {
            ByteBuffer slabId = manifestColumn.getName();

            if (total <= limit) {
                int count = execute(
                        _keyspace.prepareQuery(ColumnFamilies.SLAB, ConsistencyLevel.CL_ONE)
                                .getKey(slabId)
                                .withColumnRange(0, Constants.OPEN_SLAB_MARKER - 1, false, Integer.MAX_VALUE)
                                .getCount());
                total += count;

            } else {
                // Clients may just want to distinguish "a few" vs. "lots.  Calculate an exact count up to 'limit'
                // then estimate anything larger by counting slabs and assuming each has a full set of events.
                int slabs = execute(
                        _keyspace.prepareQuery(ColumnFamilies.MANIFEST, ConsistencyLevel.CL_ONE)
                                .getKey(channel)
                                .withColumnRange(new RangeBuilder().setStart(slabId).build())
                                .getCount());
                total += slabs * Constants.MAX_SLAB_SIZE;
                break;
            }
        }

        return total;
    }

    @Override
    public boolean moveIfFast(String fromChannel, String toChannel) {
        Iterable<Column<ByteBuffer>> manifestColumns = executePaginated(
                _keyspace.prepareQuery(ColumnFamilies.MANIFEST, ConsistencyLevel.CL_LOCAL_QUORUM)
                        .getKey(fromChannel)
                        .withColumnRange(new RangeBuilder().setLimit(50).build())
                        .autoPaginate(true));

        List<ByteBuffer> closedSlabs = Lists.newArrayList();
        boolean movedAll = true;
        for (Column<ByteBuffer> manifestColumn : manifestColumns) {
            ByteBuffer slabId = manifestColumn.getName();
            boolean open = manifestColumn.getBooleanValue();
            if (open) {
                // Can't safely re-assign open slabs to another channel since writers may still be writing.
                movedAll = false;  // All events in the open slab might be deleted, but don't check for that here.
                continue;
            }
            closedSlabs.add(slabId);
            if (closedSlabs.size() >= SLAB_MOVE_BATCH) {
                _manifestPersister.move(fromChannel, toChannel, closedSlabs, false);
                closedSlabs.clear();
            }
        }
        if (!closedSlabs.isEmpty()) {
            _manifestPersister.move(fromChannel, toChannel, closedSlabs, false);
        }

        return movedAll;
    }

    @ParameterizedTimed(type = "AstyanaxEventReaderDAO")
    @Override
    public void readAll(String channel, EventSink sink, Date since) {
        readAll(channel, since != null ? getSlabFilterSince(since, channel) : null, sink, ConsistencyLevel.CL_LOCAL_QUORUM);
    }

    void readAll(String channel, SlabFilter filter, EventSink sink, ConsistencyLevel consistency) {
        // PeekingIterator is needed so that we can look ahead and see the next slab Id
        PeekingIterator<Column<ByteBuffer>> manifestColumns = Iterators.peekingIterator(executePaginated(
                _keyspace.prepareQuery(ColumnFamilies.MANIFEST, consistency)
                        .getKey(channel)
                        .withColumnRange(new RangeBuilder().setLimit(50).build())
                        .autoPaginate(true)).iterator());

        while (manifestColumns.hasNext()) {
            Column<ByteBuffer> manifestColumn = manifestColumns.next();
            ByteBuffer slabId = manifestColumn.getName();
            ByteBuffer nextSlabId = manifestColumns.hasNext() ? manifestColumns.peek().getName() : null;
            boolean open = manifestColumn.getBooleanValue();
            if (filter != null && !filter.accept(slabId, open, nextSlabId)) {
                continue;
            }
            if (!readSlab(channel, slabId, new SlabCursor(), open, sink)) {
                break;
            }
        }
    }

    @ParameterizedTimed(type = "AstyanaxEventReaderDAO")
    @Override
    public void readNewer(String channel, EventSink sink) {
        // Algorithm notes:
        //
        // In general this method tries to read sequentially all events for a channel across multiple calls to
        // readNewer().  At the end of call N it remembers its position X using a SlabCursor object and on the next
        // call to readNewer() N+1 it starts reading at position X+1 where it left off.
        //
        // The desire to read sequentially must be balanced by the fact that reading may skip events for various
        // reasons (event already claimed, write isn't yet visible due to out-of-order writes, etc.) so this
        // method must occasionally rewind and re-read events it skipped previously in case those events are now
        // interesting.
        //
        // Two caches are used to implement this: one for a cursor position in "open" slabs (slabs that writers may
        // still be appending to) and second for a cursor position in "closed" slabs (slabs that writers are no
        // longer appending to):
        // 1. The biggest performance advantage comes from the "closed" slab cursor cache: once a closed slab has been
        //    read completely (cursor.get()==END) we can skip it entirely on the next few calls to readNewer() since we
        //    only need to re-read to handle race conditions & retry in the event of claim expiration.  So the closed
        //    slab cursor cache uses a longer TTL (10 seconds) so uninteresting closed slabs will generally be read at
        //    most every 10 seconds.
        // 2. If a slab is "open" then new content may be written at any time.  In that case, we must continuously poll
        //    the slab to discover new content.  Since we're issuing a Cassandra read no matter what, there is minimal
        //    benefit from using a cursor to start the read part-way through the slab.  So the open slab cursor cache
        //    has a very short TTL (250ms) to reduce memory requirements and minimize the latency between the time
        //    data is written and first read.

        Iterable<Column<ByteBuffer>> manifestColumns = executePaginated(
                _keyspace.prepareQuery(ColumnFamilies.MANIFEST, ConsistencyLevel.CL_ONE)
                        .getKey(channel)
                        .withColumnRange(new RangeBuilder().setLimit(50).build())
                        .autoPaginate(true));

        for (Column<ByteBuffer> manifestColumn : manifestColumns) {
            ByteBuffer slabId = manifestColumn.getName();
            boolean open = manifestColumn.getBooleanValue();
            ChannelSlab channelSlab = new ChannelSlab(channel, slabId);
            SlabCursor cursor = (open ? _openSlabCursors : _closedSlabCursors).getUnchecked(channelSlab);

            // Optimistic "can we skip this slab?" check outside the synchronized block.
            if (cursor.get() == SlabCursor.END) {
                continue;
            }

            // If multiple pollers try to query the same slab at the same time there's no reason they should do so
            // in parallel--they'll find the same events and compete for claims.  Might as well just serialize.
            // A smarter algorithm might randomize the order slabs are read to reduce contention between parallel
            // pollers, but be careful to avoid starvation.

            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (cursor) {
                if (!readSlab(channel, slabId, cursor, open, sink)) {
                    break;
                }
            }
        }
    }

    /** Returns true to keep searching for more events, false to stop searching for events. */
    private boolean readSlab(String channel, ByteBuffer slabId, SlabCursor cursor, boolean open, EventSink sink) {
        int start = cursor.get();
        if (start == SlabCursor.END) {
            return true;
        }

        boolean recent = isRecent(slabId);

        // Event add and delete write with local quorum, so read with local quorum to get a consistent view of things.
        // Using a lower consistency level could result in (a) duplicate events because we miss deletes and (b)
        // incorrectly closing or deleting slabs when slabs look empty if we miss adds.
        ColumnList<Integer> eventColumns = execute(
                _keyspace.prepareQuery(ColumnFamilies.SLAB, ConsistencyLevel.CL_LOCAL_QUORUM)
                        .getKey(slabId)
                        .withColumnRange(start, Constants.OPEN_SLAB_MARKER, false, Integer.MAX_VALUE));

        boolean searching = true;
        boolean empty = (start == 0);  // If we skipped events in the query we must assume the slab isn't empty.
        boolean more = false;
        int next = start;
        for (Column<Integer> eventColumn : eventColumns) {
            int eventIdx = eventColumn.getName();

            // Open slabs have a dummy entry at maxint that indicates that this slab is still open.
            if (eventIdx == Constants.OPEN_SLAB_MARKER) {
                break;
            }

            // Found at least one data item.
            empty = false;

            if (!searching) {
                more = true;  // There are more events to be found next time we poll this slab.
                break;
            }

            // Pass the data on to the EventSink.  It will tell us whether or not to keep searching.
            EventId eventId = AstyanaxEventId.create(channel, slabId, eventIdx);
            ByteBuffer eventData = eventColumn.getByteBufferValue();
            searching = sink.accept(eventId, eventData);
            next = eventIdx;
        }

        // Next time we query this slab start the search with last event received by the sink, repeating it.
        cursor.set(next);

        // Stale open slab?  Rare, should only happen when a writer crashes without cleaning up and closing its open
        // slabs.  Normally writers re-write the OPEN_SLAB_MARKER column on every write as a sort of heartbeat.  Readers
        // detect "stale" slabs when the open slab markers expire, and they close those slabs on behalf of the crashed writers.
        boolean hasOpenSlabMarker = !eventColumns.isEmpty() &&
                eventColumns.getColumnByIndex(eventColumns.size() - 1).getName() == Constants.OPEN_SLAB_MARKER;
        boolean stale = open && !recent && !hasOpenSlabMarker;
        if (stale) {
            _staleSlabMeter.mark();
        }

        // If the slab is currently closed or should be closed then it will never receive more data so check to see if
        // we can (a) delete it (it's empty) or at least (b) close it.
        if (empty && (!open || stale)) {
            deleteEmptySlabAsync(channel, slabId);
            open = false;
        } else if (stale) {
            closeStaleSlabAsync(channel, slabId);
            open = false;
        }

        // If we ran through all the data in a closed slab, skip this slab next time.  This is especially common with
        // badly-behaving Databus listeners that poll repeatedly but don't ack.
        if (!more && !open) {
            cursor.set(SlabCursor.END);
        }

        return searching;
    }

    /**
     * Use the age of the slabId as a heuristic to determine when we should ignore the lack of the "open slab marker".
     * <p>
     * When a slab is first created, the 'manifest' row is written with "open=true" but no columns are written to the
     * 'slab' column family.  In that initial state, scanning the slab and finding no "open slab" marker does *not*
     * mean we've found a stale slab that can be closed or deleted.  We could fix this by writing the "open slab"
     * marker before writing the manifest (not in the same batch mutation).  But since the marker would only live for 20
     * minutes without a followup write, simply checking the slabId age has exactly the same effect with fewer writes.
     */
    private boolean isRecent(ByteBuffer slabId) {
        UUID uuid = TimeUUIDSerializer.get().fromByteBuffer(slabId.duplicate());
        long age = System.currentTimeMillis() - TimeUUIDs.getTimeMillis(uuid);
        return age <= Constants.OPEN_SLAB_MARKER_TTL.getMillis();
    }

    private void closeStaleSlabAsync(final String channel, final ByteBuffer slabId) {
        _cleanupExecutor.submit(new Runnable() {
            @Override
            public void run() {
                _manifestPersister.close(channel, slabId);
            }
        });
    }

    private void deleteEmptySlabAsync(final String channel, final ByteBuffer slabId) {
        _cleanupExecutor.submit(new Runnable() {
            @Override
            public void run() {
                _manifestPersister.delete(channel, slabId);
            }
        });
    }

    /** Executes a {@code RowQuery} with {@code autoPaginate(true)} repeatedly as necessary to fetch all pages. */
    private <K, C> Iterable<Column<C>> executePaginated(final RowQuery<K, C> query) {
        return OneTimeIterable.wrap(Iterators.concat(new AbstractIterator<Iterator<Column<C>>>() {
            @Override
            protected Iterator<Column<C>> computeNext() {
                ColumnList<C> page = execute(query);
                return !page.isEmpty() ? page.iterator() : endOfData();
            }
        }));
    }

    private <R> R execute(Execution<R> execution) {
        OperationResult<R> operationResult;
        try {
            operationResult = execution.execute();
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }
        return operationResult.getResult();
    }

    /**
     * When all events from a slab have been read via {@link #readNewer(String, EventSink)} _closedSlabCursors
     * caches that the slab is empty for 10 seconds.  When a slab is marked as unread update the cursor, if present,
     * such that it is rewound far enough to read the unread event again.
     */
    @Override
    public void markUnread(String channel, Collection<EventId> events) {
        // For each slab keep track of the earliest index for each unread event.
        ConcurrentMap<ChannelSlab, Integer> channelSlabs = Maps.newConcurrentMap();
        for (EventId event : events) {
            AstyanaxEventId astyanaxEvent = (AstyanaxEventId) event;
            checkArgument(channel.equals(astyanaxEvent.getChannel()));
            channelSlabs.merge(new ChannelSlab(channel, astyanaxEvent.getSlabId()), astyanaxEvent.getEventIdx(), Ints::min);
        }

        for (Map.Entry<ChannelSlab, Integer> entry : channelSlabs.entrySet()) {
            ChannelSlab channelSlab = entry.getKey();
            int eventIdx = entry.getValue();
            // Get the closed slab cursor, if any
            SlabCursor cursor = _closedSlabCursors.getIfPresent(channelSlab);
            // If the cursor exists and is beyond the lowest unread index, rewind it
            if (cursor != null && (cursor.get() == SlabCursor.END || cursor.get() > eventIdx)) {
                // Synchronize on the cursor before updating it to avoid concurrent updates with a read
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (cursor) {
                    if (cursor.get() == SlabCursor.END || cursor.get() > eventIdx) {
                        cursor.set(eventIdx);
                    }
                }
            }
        }
    }

    private static class SlabCursor {
        static final int END = Integer.MAX_VALUE;

        private int _value;

        public int get() {
            return _value;
        }

        public void set(int value) {
            _value = value;
        }
    }

    @VisibleForTesting
    protected SlabFilter getSlabFilterSince(final Date since, final String channel) {
        return new SlabFilter() {
            boolean foundStartingSlab = false;
            final UUID sinceUUID = TimeUUIDs.uuidForTimestamp(since);
            @Override
            public boolean accept(ByteBuffer slabId, boolean open, ByteBuffer nextSlabId) {
                UUID nextSlabUUID = !foundStartingSlab && nextSlabId != null ?
                        TimeUUIDSerializer.get().fromByteBuffer(nextSlabId.duplicate())
                        : null;
                // If the nextSlab's UUID is less than the desired timestamp, then skip reading events from this slab
                if (nextSlabUUID != null && TimeUUIDs.compareTimestamps(nextSlabUUID, sinceUUID) < 0) {
                    return false;
                }
                if (!foundStartingSlab) {
                    // Log the slab that we start reading from
                    foundStartingSlab = true;
                    _log.info("Starting to replay {} from slabid {}, for since timestamp of {}", channel,
                            UUIDSerializer.get().fromByteBuffer(slabId),  ISO_FORMATTER.print(since.getTime()));
                }
                return true;
            }
        };
    }

    /**
     * Uniquely identifies a cursor for a slab based on the channel the slab belongs to.  This way if a slab is moved
     * from one channel to another as part of a queue or databus move operation the position in the slab for each
     * channel is maintained independently.
     */
    private static class ChannelSlab {
        private final String _channel;
        private final ByteBuffer _slabId;

        private ChannelSlab(String channel, ByteBuffer slabId) {
            _channel = channel;
            _slabId = slabId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ChannelSlab)) {
                return false;
            }

            ChannelSlab that = (ChannelSlab) o;

            return _slabId.equals(that._slabId) && _channel.equals(that._channel);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(_channel, _slabId);
        }

        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("channel", _channel)
                    .add("slabId", ByteBufferUtil.bytesToHex(_slabId.asReadOnlyBuffer()))
                    .toString();
        }
    }
}
