package com.bazaarvoice.emodb.event.db.astyanax;

import com.bazaarvoice.emodb.common.api.Ttls;
import com.bazaarvoice.emodb.common.cassandra.CassandraKeyspace;
import com.bazaarvoice.emodb.common.dropwizard.metrics.ParameterizedTimed;
import com.bazaarvoice.emodb.event.api.ChannelConfiguration;
import com.bazaarvoice.emodb.event.db.EventId;
import com.bazaarvoice.emodb.event.db.EventSink;
import com.bazaarvoice.emodb.event.db.EventWriterDAO;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;
import com.google.inject.Inject;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.thrift.ThriftUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class AstyanaxEventWriterDAO implements EventWriterDAO {
    private final CassandraKeyspace _keyspace;
    private final SlabAllocator _slabAllocator;
    private final AstyanaxEventReaderDAO _eventReaderDAO;
    private final ChannelConfiguration _channelConfiguration;

    @Inject
    public AstyanaxEventWriterDAO(CassandraKeyspace keyspace, SlabAllocator slabAllocator,
                                  AstyanaxEventReaderDAO eventReaderDAO, ChannelConfiguration channelConfiguration) {
        _keyspace = keyspace;
        _slabAllocator = slabAllocator;
        _eventReaderDAO = eventReaderDAO;
        _channelConfiguration = channelConfiguration;
    }

    @ParameterizedTimed(type="AstyanaxEventWriterDAO")
    @Override
    public void addAll(Multimap<String, ByteBuffer> eventsByChannel, @Nullable EventSink sink) {
        checkNotNull(eventsByChannel, "eventsByChannel");

        List<SlabAllocation> allocationsToRelease = Lists.newArrayList();
        try {

            BatchUpdate update = new BatchUpdate(_keyspace, ConsistencyLevel.CL_LOCAL_QUORUM,
                    Constants.MUTATION_MAX_ROWS, Constants.MUTATION_MAX_COLUMNS);

            for (Map.Entry<String, Collection<ByteBuffer>> entry : eventsByChannel.asMap().entrySet()) {
                String channel = entry.getKey();
                Collection<ByteBuffer> events = entry.getValue();
                Iterator<ByteBuffer> eventsIter = events.iterator();

                Duration eventTtl = _channelConfiguration.getEventTtl(channel);

                int remaining = events.size();
                PeekingIterator<Integer> eventSizes = Iterators.peekingIterator(Collections2.transform(events,
                    new Function<ByteBuffer,Integer>() {
                        @Override
                        public Integer apply(ByteBuffer input) {
                            return input.limit();
                        }
                    }).iterator());

                while (remaining > 0) {
                    // Each allocation will be from a different slab and hence a different row.
                    SlabAllocation allocation = _slabAllocator.allocate(channel, remaining, eventSizes);

                    // Defensive coding against possible slab allocation bugs, make sure slab allocation
                    // is not larger than number of events to put into it
                    assert allocation.getLength() <= remaining;

                    ByteBuffer slabId = allocation.getSlabId();
                    BatchUpdate.Row<ByteBuffer, Integer> row = update.updateRow(ColumnFamilies.SLAB, slabId,
                            new Function<BatchUpdate.Row<ByteBuffer, Integer>, Void>() {
                                @Override
                                public Void apply(BatchUpdate.Row<ByteBuffer, Integer> row) {
                                    // Add a marker column indicating this slab is still active and open.
                                    row.putColumn(Constants.OPEN_SLAB_MARKER, ThriftUtils.EMPTY_BYTE_BUFFER,
                                            Ttls.toSeconds(Constants.OPEN_SLAB_MARKER_TTL, 1, null));
                                    return null;
                                }
                            });

                    // Loop through all slots in the slab and assign a channel event to each slot
                    for (int i = 0; i < allocation.getLength(); i++) {
                        int eventIdx = allocation.getOffset() + i;

                        // we don't have to check for a drained iterator because we know that the slab allocation
                        // will never have more slots than there are events to put in them, see assertion at line 82
                        ByteBuffer eventData = eventsIter.next();
                        row.putColumn(eventIdx, eventData, Ttls.toSeconds(eventTtl, 1, null));

                        if (sink != null && !sink.accept(AstyanaxEventId.create(channel, slabId, eventIdx), eventData)) {
                            sink = null;  // Sink isn't interested in more events.
                        }
                    }

                    remaining -= allocation.getLength();
                    allocationsToRelease.add(allocation);
                }
            }

            update.finish();

        } finally {
            // We may release the slab allocations only after the event data has been committed to Cassandra.  Otherwise
            // we risk closing slabs in the manifest too early, causing readers to miss events.
            for (SlabAllocation allocation : allocationsToRelease) {
                allocation.release();
            }
        }
    }

    @Override
    public void delete(String channel, Collection<EventId> eventIds) {
        checkNotNull(channel, "channel");
        checkNotNull(eventIds, "eventIds");

        ListMultimap<ByteBuffer, Integer> eventsBySlab = ArrayListMultimap.create();
        for (EventId eventId : eventIds) {
            AstyanaxEventId eventIdImpl = (AstyanaxEventId) eventId;
            checkArgument(channel.equals(eventIdImpl.getChannel()));
            eventsBySlab.put(eventIdImpl.getSlabId(), eventIdImpl.getEventIdx());
        }

        // We might be able to use weak consistency since we're allowed to forget deletes and repeat events.  But we'd
        // need to measure it in production to see how frequently weak consistency would cause events to repeat.
        BatchUpdate update = new BatchUpdate(_keyspace, ConsistencyLevel.CL_LOCAL_QUORUM,
                Constants.MUTATION_MAX_ROWS, Constants.MUTATION_MAX_COLUMNS);

        for (Map.Entry<ByteBuffer, Collection<Integer>> entry : eventsBySlab.asMap().entrySet()) {
            ByteBuffer slabId = entry.getKey();
            Collection<Integer> eventIdxs = entry.getValue();

            BatchUpdate.Row<ByteBuffer, Integer> row = update.updateRow(ColumnFamilies.SLAB, slabId);
            for (Integer eventIdx : eventIdxs) {
                row.deleteColumn(eventIdx);
            }
        }

        update.finish();
    }

    @Override
    public void deleteAll(final String channel) {
        checkNotNull(channel, "channel");

        // There isn't really a good way to delete events en masse because (a) there may be many processes writing to
        // the queue, (b) it's impractical to expect all those processes to close all their open slabs, therefore (c)
        // we must delete the events in a way that leaves the open slabs in a valid state, therefore (d) we can't
        // simply delete all the slab rows since we must preserve the OPEN_SLAB_MARKER column.  Therefore, purge
        // of open slabs is implemented by reading all the events one-by-one and deleting them.
        final BatchUpdate update = new BatchUpdate(_keyspace, ConsistencyLevel.CL_LOCAL_QUORUM,
                Constants.MUTATION_MAX_ROWS, Constants.MUTATION_MAX_COLUMNS);

        class Deleter implements SlabFilter, EventSink {
            private BatchUpdate.Row<String, ByteBuffer> _manifestRow;
            private BatchUpdate.Row<ByteBuffer, Integer> _slabRow;
            private ByteBuffer _currentSlabId;

            void run() {
                _manifestRow = update.updateRow(ColumnFamilies.MANIFEST, channel);
                _eventReaderDAO.readAll(channel, this, this, false);
            }

            // SlabFilter interface, called before scanning events in a slab.
            @Override
            public boolean accept(ByteBuffer slabId, boolean open, ByteBuffer nextSlabId) {
                _currentSlabId = slabId;
                _slabRow = update.updateRow(ColumnFamilies.SLAB, slabId);

                if (open) {
                    // Open slab: read and delete the events individually.
                    return true;
                } else {
                    // Closed slab: delete the entire slab without reading it.
                    _slabRow.deleteRow();
                    _manifestRow.deleteColumn(slabId);
                    return false;
                }
            }

            // EventSink interface, called on individual events in a slab.
            @Override
            public boolean accept(EventId eventId, ByteBuffer eventData) {
                AstyanaxEventId eventIdImpl = (AstyanaxEventId) eventId;
                checkState(_currentSlabId.equals(eventIdImpl.getSlabId()));
                int eventIdx = eventIdImpl.getEventIdx();

                _slabRow.deleteColumn(eventIdx);

                return true; // Keep looking for more events
            }
        }
        new Deleter().run();

        update.finish();
    }
}
