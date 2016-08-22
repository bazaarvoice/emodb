package com.bazaarvoice.emodb.sortedq.core;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sortedq.db.QueueDAO;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.collect.TreeMultimap;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.UUID;

public class InMemoryQueueDAO implements QueueDAO {
    private final Table<String, UUID, String> _manifest = HashBasedTable.create();
    private final TreeMultimap<UUID, ByteBuffer> _records =
            TreeMultimap.create(TimeUUIDs.ordering(), ByteBufferOrdering.INSTANCE);
    private long _numSegmentWrites;
    private long _numSegmentDeletes;
    private long _numRecordWrites;
    private long _numRecordDeletes;
    private final Supplier<Boolean> _crashDecider;
    private final Random _random;

    public InMemoryQueueDAO() {
        this(Suppliers.ofInstance(false), null);
    }

    public InMemoryQueueDAO(Supplier<Boolean> crashDecider, Random random) {
        _crashDecider = crashDecider;
        _random = random;
    }

    public Table<String, UUID, String> getManifest() {
        return _manifest;
    }

    public TreeMultimap<UUID, ByteBuffer> getRecords() {
        return _records;
    }

    public long getNumSegmentWrites() {
        return _numSegmentWrites;
    }

    public long getNumSegmentDeletes() {
        return _numSegmentDeletes;
    }

    public long getNumRecordWrites() {
        return _numRecordWrites;
    }

    public long getNumRecordDeletes() {
        return _numRecordDeletes;
    }

    @Override
    public Iterator<String> listQueues() {
        return _manifest.rowKeySet().iterator();
    }

    @Override
    public Map<UUID, String> loadSegments(String queue) {
        return ImmutableMap.copyOf(_manifest.row(queue));
    }

    @Nullable
    @Override
    public ByteBuffer findMinRecord(UUID dataId, @Nullable ByteBuffer from) {
        NavigableSet<ByteBuffer> records = _records.get(dataId);
        return from == null ? (records.isEmpty() ? null : records.first()) : records.ceiling(from);
    }

    @Override
    public Map<UUID, ByteBuffer> findMaxRecords(Collection<UUID> dataIds) {
        ImmutableMap.Builder<UUID, ByteBuffer> result = ImmutableMap.builder();
        for (UUID dataId : dataIds) {
            NavigableSet<ByteBuffer> records = _records.get(dataId);
            if (!records.isEmpty()) {
                result.put(dataId, records.last());
            }
        }
        return result.build();
    }

    @Override
    public Iterator<ByteBuffer> scanRecords(UUID dataId, @Nullable ByteBuffer from, @Nullable ByteBuffer to,
                                            int batchSize, int limit) {
        NavigableSet<ByteBuffer> segments = _records.get(dataId);
        if (segments == null) {
            return Iterators.emptyIterator();
        }

        if (from != null) {
            segments = segments.tailSet(from, true);
        }
        if (to != null) {
            segments = segments.headSet(to, false);
        }
        return ImmutableList.copyOf(Iterators.limit(segments.iterator(), limit)).iterator();
    }

    @Override
    public UpdateRequest prepareUpdate(final String queue) {
        return new UpdateRequest() {
            private final List<Runnable> _mutations = Lists.newArrayList();

            @Override
            public UpdateRequest writeSegment(final UUID segment, final String internalState) {
                _mutations.add(new Runnable() {
                    @Override
                    public void run() {
                        _manifest.row(queue).put(segment, internalState);
                        _numSegmentWrites++;
                    }
                });
                return this;
            }

            @Override
            public UpdateRequest deleteSegment(final UUID segment, final UUID dataId) {
                _mutations.add(new Runnable() {
                    @Override
                    public void run() {
                        _manifest.row(queue).remove(segment);
                        _numSegmentDeletes++;
                    }
                });
                _mutations.add(new Runnable() {
                    @Override
                    public void run() {
                        _records.removeAll(dataId);
                    }
                });
                return this;
            }

            @Override
            public UpdateRequest writeRecords(final UUID dataId, final Collection<ByteBuffer> records) {
                if (!records.isEmpty()) {
                    _mutations.add(new Runnable() {
                        @Override
                        public void run() {
                            _records.get(dataId).addAll(records);
                            _numRecordWrites += records.size();
                        }
                    });
                }
                return this;
            }

            @Override
            public UpdateRequest deleteRecords(final UUID dataId, final Collection<ByteBuffer> records) {
                if (!records.isEmpty()) {
                    _mutations.add(new Runnable() {
                        @Override
                        public void run() {
                            _records.get(dataId).removeAll(records);
                            _numRecordDeletes += records.size();
                        }
                    });
                }
                return this;
            }

            @Override
            public void execute() {
                // Decide whether to simulate a failure.  And if so, how many operations should succeed before failing?
                boolean crash = _crashDecider.get();
                int crashAfter = crash ? _random.nextInt(_mutations.size() + 1) : Integer.MAX_VALUE;

                if (crashAfter == 0) {
                    throw new SimulatedFailureException();
                }

                // Apply the mutations on random order in case we crash part way through, to test algorithm
                // behavior when some row mutations succeed but others don't.
                if (_random != null) {
                    Collections.shuffle(_mutations, _random);
                }
                for (Runnable mutation : _mutations) {
                    mutation.run();
                    if (--crashAfter == 0) {
                        throw new SimulatedFailureException();
                    }
                }
            }
        };
    }

    // For debugging
    @Override
    public String toString() {
        return String.format("#segs=%,d; #recs=%,d; #seg writes=%,d; #seg deletes=%,d; #rec writes=%,d; #rec deletes=%,d",
                _manifest.size(), _records.size(), _numSegmentWrites, _numSegmentDeletes, _numRecordWrites, _numRecordDeletes);
    }
}
