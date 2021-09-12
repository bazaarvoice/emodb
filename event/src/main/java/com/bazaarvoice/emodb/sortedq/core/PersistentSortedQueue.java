package com.bazaarvoice.emodb.sortedq.core;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sortedq.api.Consumer;
import com.bazaarvoice.emodb.sortedq.api.SortedQueue;
import com.bazaarvoice.emodb.sortedq.db.QueueDAO;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.apache.cassandra.utils.ByteBufferUtil;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.bazaarvoice.emodb.sortedq.db.QueueDAO.UpdateRequest;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * An implementation of {@link SortedQueue} backed by a Cassandra-like data store.
 * <p>
 * All records are stored persistently in a set of "segments" where each segment is responsible for a certain key
 * range.  Data with the segment is sorted and deduplicated.  With Cassandra, each segment is a separate row, each
 * record is the key of a separate column, and Cassandra takes care of sorting and deduplicating within the row.
 * <p>
 * While Cassandra theoretically supports extremely large rows, performance tends to drop off with large rows due
 * to compaction overhead.  Once enough data has been written to a segment that it is larger than a certain threshold
 * (see {@link #SPLIT_THRESHOLD_BYTES}) the segment will be split into several smaller segments.
 * <p>
 * This implementation is <em>not</em> safe to use concurrently, ie. from multiple servers.  While the Java object is
 * thread-safe, it assumes that it is the only writer to the persistent store.  Concurrent writes by other processes
 * will cause the in-memory state and persistent state to diverge, leading to bugs and potential data loss.
 */
public class PersistentSortedQueue implements SortedQueue {
    /**
     * Try to keep segments not much bigger than 256MB to spread data around the ring, and since rows that are
     * too large slow Cassandra compaction, repair etc.
     */
    private static final int SPLIT_THRESHOLD_BYTES = 256 * 1024 * 1024;

    /** When splitting, process 64k at a time. */
    private static final int SPLIT_WORK_BYTES = 64 * 1024;

    private static final Ordering<ByteBuffer> ORDERING = ByteBufferOrdering.INSTANCE.nullsFirst();

    @VisibleForTesting
    static Random RANDOM = new Random();

    /** Special token that means a higher value than any other byte buffer. */
    private static final ByteBuffer MAX = ByteBuffer.allocate(0);

    private static Meter _drainToTimeout = null;

    private final QueueDAO _dao;
    private final String _name;
    private final NavigableMap<ByteBuffer, Segment> _segmentMap = Maps.newTreeMap(ORDERING);
    private final Set<Segment> _dirtyList = Sets.newLinkedHashSet();
    private final SplitQueue<Segment> _splitQueue = new SplitQueue<>();
    private final long _splitThresholdBytes;
    private final long _splitWorkBytes;
    private final Budget _splitBudget = new Budget();  // Measured in bytes
    private final Set<UUID> _uuidsToRecycle = newBoundedHashSet(10);
    private Segment _readPosition;
    private final ReadWriteLock _readWriteLock = new ReentrantReadWriteLock();
    private final Lock _readLock = _readWriteLock.readLock();
    private final Lock _writeLock = _readWriteLock.writeLock();
    private volatile boolean _readOnly;
    private volatile boolean _empty;

    /** Creates a new persistent sorted queue that stores its persistent state under the specified queue name. */
    @AssistedInject
    public PersistentSortedQueue(@Assisted String name, @Assisted QueueDAO dao, MetricRegistry metricRegistry) {
        this(name, false, dao, metricRegistry);
    }

    /**
     * Creates a sorted queue that loads its initial state in a read-only mode.  This can be used to inspect the
     * current state of a sorted queue that can't be modified for some reason, perhaps because it is owned by
     * another server.
     */
    @AssistedInject
    public PersistentSortedQueue(@Assisted String name, @Assisted boolean readOnly, @Assisted QueueDAO dao, MetricRegistry metricRegistry) {
        this(name, readOnly, SPLIT_THRESHOLD_BYTES, SPLIT_WORK_BYTES, dao, metricRegistry);
    }

    @VisibleForTesting
    PersistentSortedQueue(String name, boolean readOnly, long splitThresholdBytes, int splitWorkBytes, QueueDAO dao, MetricRegistry metricRegistry) {
        _dao = checkNotNull(dao, "dao");
        _name = checkNotNull(name, "name");
        _readOnly = readOnly;
        _splitThresholdBytes = splitThresholdBytes;
        _splitWorkBytes = splitWorkBytes;
        _drainToTimeout = metricRegistry.meter(MetricRegistry.name("bv.emodb.queue", "PersistentSortedQueue", "drainToTimeout"));

        // Load the initial state from disk.
        load();

        // Cache the empty state so we can read it w/o obtaining locks.
        updateEmpty();
    }

    @Override
    public boolean isReadOnly() {
        return _readOnly;
    }

    /** Prevent all future writes to the database.  Once set, this cannot be unset. */
    @Override
    public void setReadOnly() {
        _readOnly = true;  // not synchronized so it takes effect immediately
    }

    private void checkWritesAllowed() {
        if (_readOnly) {
            throw new ReadOnlyQueueException("Queue has been marked read-only, writes are not allowed: " + _name);
        }
    }

    @Override
    public boolean isEmpty() {
        return _empty;
    }

    private void updateEmpty() {
        _empty = _segmentMap.isEmpty();
    }

    @Override
    public long sizeEstimate() {
        _readLock.lock();
        try {
            long count = 0;
            for (Segment seg : _segmentMap.values()) {
                count += seg.cardinality();
            }
            return count;
        } finally {
            _readLock.unlock();
        }
    }

    @Override
    public void addAll(Collection<ByteBuffer> records) {
        checkNotNull(records, "records");
        checkWritesAllowed();

        if (records.isEmpty()) {
            return;
        }

        int bytesToAdd = sumLengths(records);

        // Do some splitting.  For every byte that's written, allow moving 2 bytes as part of splitting.
        // The idea is to spread the work of splitting across write requests so that splitting is done in
        // small chunks that don't hold locks for an extended period of time.
        _splitBudget.credit(bytesToAdd * 2);
        // If we've accumulated enough budget to do some split work, go do it.
        while (_splitBudget.debitIfAvailable(_splitWorkBytes)) {
            splitSegments(_splitWorkBytes);
        }

        // Splitting is done.  The rest below is the actual "addAll" implementation.
        _writeLock.lock();
        try {
            // Sort so all writes for a segment are processed together as expected by SegmentMods.
            List<ByteBuffer> sortedRecords = ORDERING.sortedCopy(records);

            // Figure out which segment each record belongs to and update the segment.
            SegmentMods segmentMods = new SegmentMods();
            Segment created = null;
            for (ByteBuffer record : sortedRecords) {
                Segment seg = valueOrNull(_segmentMap.floorEntry(record));
                if (seg == null) {
                    // No segment exists that would contain the specified record.  Create one.  This happens at most
                    // once per addAll() since the segment we create has "min=<null>" which covers all uncovered range.
                    if (created == null) {
                        created = newSegment(null/*min*/);
                        segmentMods.with(created).create();  // This will add the segment to _segmentMap at commit().
                    }
                    seg = created;
                }

                segmentMods.with(seg).write(record);
            }
            segmentMods.commit();

            // Prioritize splitting of segments being written to.
            if (!_splitQueue.isEmpty()) {
                // Pick one at random.  Odds are we'll pick the one that's written to most frequently.
                ByteBuffer randomRecord = sortedRecords.get(RANDOM.nextInt(sortedRecords.size()));
                _splitQueue.prioritize(valueOrNull(_segmentMap.floorEntry(randomRecord)));
            }
        } finally {
            _writeLock.unlock();
        }
    }

    @Override
    public Iterator<ByteBuffer> scan(final @Nullable ByteBuffer fromInclusive, long limit) {
        checkArgument(limit > 0, "Limit must be >0");
        final LimitCounter remaining = new LimitCounter(limit);
        return remaining.limit(Iterators.concat(new AbstractIterator<Iterator<ByteBuffer>>() {
            private ByteBuffer _pos = fromInclusive;

            @Override
            protected Iterator<ByteBuffer> computeNext() {
                if (_pos == MAX) {
                    return endOfData();
                }

                _readLock.lock();
                try {
                    // Find the segment that covers our starting position, or the following segment if none.
                    Segment seg = valueOrNull(_segmentMap.floorEntry(_pos));
                    if (seg == null) {
                        seg = valueOrNull(_segmentMap.higherEntry(_pos));
                    }
                    if (seg == null) {
                        return endOfData();
                    }

                    // Compute bounds and execute the query.
                    Segment next = valueOrNull(_segmentMap.higherEntry(seg.getMin()));
                    ByteBuffer from = ORDERING.max(_pos, seg.getMin());
                    ByteBuffer to = (next != null) ? next.getMin() : null;
                    int segmentLimit = (int) Math.min(remaining.remaining(), Integer.MAX_VALUE);  // A single segment can't have more than 2GB entries.
                    Iterator<ByteBuffer> iter = _dao.scanRecords(seg.getDataId(), from, to, scanBatchSize(), segmentLimit);

                    // Advance to the next segment.
                    _pos = to != null ? to : MAX;

                    return iter;
                } finally {
                    _readLock.unlock();
                }
            }
        }));
    }

    @Override
    public void drainTo(Consumer consumer, long limit) {
        drainTo(consumer, limit, null);
    }

    @Override
    public void drainTo(Consumer consumer, long limit, @Nullable Duration timeout) {
        checkNotNull(consumer, "consumer");
        checkArgument(limit > 0, "Limit must be >0");

        long timeoutAt = (timeout != null) ? System.currentTimeMillis() + timeout.toMillis() : Long.MAX_VALUE;

        try {
            if (timeout == null) {
                _writeLock.lock();
            } else if (!_writeLock.tryLock(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                _drainToTimeout.mark();
                return;
            }
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
        try {
            checkWritesAllowed();

            if (_readPosition == null || _readPosition.isDeleted()) {
                _readPosition = firstSegment();  // If the read position is obviously invalid due to a previous exception, fix it.
            }
            if (_readPosition == null) {
                return;  // Nothing to do
            }

            long remaining = limit;
            int batchSize = scanBatchSize();
            List<ByteBuffer> consumerBatch = Lists.newArrayList();
            SegmentMods segmentMods = new SegmentMods();

            // Be careful reading Segment properties after wrap-around--changes may not have been committed to memory.
            // So, for example, seg.isDeleted() might return false even though the segment is deleted in SegmentMods.
            ByteBuffer startReadPosition = _readPosition.getMin();
            Segment seg = _readPosition;
            boolean wrapped = false;
            outerLoop:
            while (seg != null && remaining > 0 && !(wrapped && ORDERING.compare(seg.getMin(), startReadPosition) >= 0)) {
                // Don't query the segment if we've exhausted our allotted time.
                if (System.currentTimeMillis() >= timeoutAt) {
                    _drainToTimeout.mark();
                    break;
                }

                // Query for one more item than we need so we can tell when we consume an entire segment.
                int segmentLimit = (int) Math.min(remaining, Integer.MAX_VALUE - 1);  // A single segment can't have more than 2GB entries.
                Segment next = valueOrNull(_segmentMap.higherEntry(seg.getMin()));
                ByteBuffer from = seg.getMin();
                ByteBuffer to = (next != null) ? next.getMin() : null;
                PeekingIterator<ByteBuffer> iter = Iterators.peekingIterator(
                        _dao.scanRecords(seg.getDataId(), from, to, batchSize, segmentLimit + 1));

                // Loop through the records and pass them to the consumer in batches.
                while (iter.hasNext() && remaining > 0) {
                    ByteBuffer record = iter.next();

                    // Pass the record to the consumer and delete it from disk.
                    consumerBatch.add(record);
                    segmentMods.with(seg).delete(record).setMin(iter.hasNext() ? iter.peek() : MAX);
                    remaining--;

                    // If we've accumulated enough records, pass a batch through to the consumer.
                    if (consumerBatch.size() >= batchSize) {
                        consumer.consume(consumerBatch);
                        consumerBatch.clear();

                        // Now that the consumer has processed the records we may delete them disk.
                        segmentMods.commit();

                        // After each call to consumer.consume() check whether we've exhausted our allotted time.
                        if (System.currentTimeMillis() >= timeoutAt) {
                            _drainToTimeout.mark();
                            break outerLoop;
                        }
                    }
                }
                if (remaining <= 0) {
                    break;
                }

                // Advance to the next segment, wrapping around as necessary.
                seg = next;
                if (seg == null && !wrapped) {
                    seg = firstSegment();
                    wrapped = true;
                }
            }

            // Flush the last batch of records through, delete them from disk and update the segment map.
            if (!consumerBatch.isEmpty()) {
                consumer.consume(consumerBatch);
                segmentMods.commit();
            }

            // Set the read position to where we stopped reading.
            _readPosition = seg != null ? ceilingSegment(seg.getMin()) : null;
        } finally {
            _writeLock.unlock();
        }
    }

    @Override
    public void clear() {
        _writeLock.lock();
        try {
            checkWritesAllowed();

            SegmentMods mods = new SegmentMods();
            for (Segment segment : _segmentMap.values()) {
                mods.with(segment).delete();
            }
            mods.commit();
        } finally {
            _writeLock.unlock();
        }
    }

    private void splitSegments(long budgetBytes) {
        _writeLock.lock();
        try {
            if (!_splitQueue.isEmpty()) {
                Segment seg;
                while (budgetBytes > 0 && (seg = _splitQueue.cycle()) != null) {
                    budgetBytes = splitSegment(seg, budgetBytes);
                }
            }

            // Don't accumulate split budget when there are no pending split operations.  A large accumulated budget
            // would otherwise cause us to do too much split work all at once the next time a record needs to be split.
            if (_splitQueue.isEmpty()) {
                _splitBudget.clear();
            }
        } finally {
            _writeLock.unlock();
        }
    }

    private long splitSegment(Segment src, long budgetBytes) {
        checkWritesAllowed();

        // If the dirty list is non-empty because of a previous exception, flush those changes to disk
        // to ensure we're in a good state before proceeding.
        new SegmentMods().commit();

        int batchSize = scanBatchSize();
        ByteBuffer startReadPosition = (_readPosition != null) ? _readPosition.getMin() : null;

        SegmentMods srcMods = new SegmentMods();
        SegmentMods dstMods = new SegmentMods();

        // Find the destination segment that we'll copy entries to.
        Segment dst = valueOrNull(_segmentMap.lowerEntry(src.getMin()));

        // Query for all the records in this segment.
        Segment next = valueOrNull(_segmentMap.higherEntry(src.getMin()));
        ByteBuffer from = src.getMin();
        ByteBuffer toExclusive = (next != null) ? next.getMin() : null;
        Iterator<ByteBuffer> iter = _dao.scanRecords(src.getDataId(), from, toExclusive, batchSize, Integer.MAX_VALUE);

        while (iter.hasNext() && budgetBytes > 0) {
            ByteBuffer record = iter.next();

            // Create a new segment if starting on a brand new split or if we've copied enough bytes to the previous dst.
            if (src.onSplitWork(record.remaining()) || dst == null) {
                // Create a new destination segment.  Do not add it to the segment map just yet because it
                // will overlap/collide with the source segment.
                dst = newSegment(dst == null ? null : record);
                Runnable finisher = dstMods.with(dst).createProvisionally();
                srcMods.runAtCommit(finisher);  // Runs after src setMin but before writing to disk.
            }

            // Write to dst and delete from src.
            dstMods.with(dst).write(record);
            srcMods.with(src).delete(record);
            budgetBytes -= record.remaining();
        }
        srcMods.with(src).setMin(iter.hasNext() ? iter.next() : MAX);

        // We must execute things in the following order to ensure consistent state in the event of write failures:
        // 1. Write dst records and segment to disk.  If this fails then nothing important has changed, just fail.
        // 2. Move src segment in the segment map.
        // 3. Add dst segments to the segment map since the src segment is now guaranteed out of the way.
        // 4. Delete src records and update/delete src segment on disk.  If this fails then we don't actually know
        //    if the write took place.  Either way, it's safe to proceed as if it did take place and the dst
        //    segments now own the range that copies copied from src.
        dstMods.commit();
        srcMods.commit();

        // Adjust the read position to point to the segment that contains its start location, but don't
        // move the read position backwards to ensure readers loop fairly around the key space.
        _readPosition = ceilingSegment(startReadPosition);

        return budgetBytes;  // If we have budget remaining we can spend it splitting other segments.
    }

    private void load() {
        if (_readOnly) {
            loadReadOnly();
        } else {
            loadAndFixup();
        }
    }

    /**
     * Loads the segment persistent state from disk and initializes in-memory state such that read-only
     * operations like {@link #sizeEstimate()} and {@link #scan(java.nio.ByteBuffer, long)} will return
     * reasonable results, but doesn't bother setting up the split queue or removing overlap between
     * segments since those are only strictly required for full read/write operation.
     */
    private void loadReadOnly() {
        checkState(_segmentMap.isEmpty()); // Only supported for newly allocated queue objects
        checkState(_readOnly);

        // Load the segment persistent state from disk.
        for (Map.Entry<UUID, String> entry : _dao.loadSegments(_name).entrySet()) {
            UUID id = entry.getKey();
            Segment seg = newSegmentFromSnapshot(id, entry.getValue());

            // Don't bother sorting segments, adjusting min or removing overlap.  They're not necessary
            // when loading a read-only persistent queue.
            while (_segmentMap.containsKey(seg.getMin())) {
                seg.setMin(successor(seg.getMin()));
            }
            _segmentMap.put(seg.getMin(), seg);
        }
    }

    private void loadAndFixup() {
        checkState(_segmentMap.isEmpty()); // Only supported for newly allocated queue objects
        checkState(!_readOnly); // Note this method is called from the constructor so _readOnly can't change until it completes.

        // Load the segment persistent state from disk.  If none, this is a new queue.
        Map<UUID, String> segmentStates = _dao.loadSegments(_name);
        if (segmentStates.isEmpty()) {
            return;
        }

        // The segment persistent state may be slightly out-of-date with respect to the actual queue data.
        // Things like row counts can be off without major consequences, but to avoid losing data we must
        // ensure that (a) min is accurate and (b) segments don't overlap.

        // Create the in-memory Segment objects from saved data.
        List<Segment> segments = Lists.newArrayList();
        List<UUID> dataIds = Lists.newArrayList();
        for (Map.Entry<UUID, String> entry : segmentStates.entrySet()) {
            UUID id = entry.getKey();
            Segment seg = newSegmentFromSnapshot(id, entry.getValue());

            ByteBuffer min = _dao.findMinRecord(seg.getDataId(), seg.getMin());

            // If the segment has no records then throw it away.
            if (min == null) {
                deleteSegment(seg);
                continue;
            }

            // Segment min only moves right, so "max(snapshot min, actual min)" is the most up-to-date value.
            if (seg.getMin() != null) {
                seg.setMin(ORDERING.max(seg.getMin(), min));
            }

            segments.add(seg);
            dataIds.add(seg.getDataId());
        }

        // Sort the segments from min to max.  If there are ties, prefer shorter segments then newer segments.
        final Map<UUID, ByteBuffer> maxRecords = _dao.findMaxRecords(dataIds);
        Collections.sort(segments, new Comparator<Segment>() {
            @Override
            public int compare(Segment left, Segment right) {
                ByteBuffer leftMax = maxRecords.get(left.getDataId());
                ByteBuffer rightMax = maxRecords.get(right.getDataId());
                return ComparisonChain.start()
                        .compare(left.getMin(), right.getMin(), ORDERING)  // Ascending
                        .compare(leftMax, rightMax, ORDERING)                     // Ascending
                        .compare(right.getDataId(), left.getDataId(), TimeUUIDs.ordering())  // Descending
                        .result();
            }
        });

        // Loop through the segments from lowest to highest and eliminate any overlap in the data in the segments.
        ByteBuffer prevMax = null;
        for (Segment seg : segments) {
            ByteBuffer max = maxRecords.get(seg.getDataId());
            if (max == null) {
                deleteSegment(seg);
                continue;   // Unlikely but theoretically possible due to race conditions.
            }

            // If this segment has data that overlaps the previous segment, eliminate the overlap by moving
            // the overlapping data to the previous segment.  This may end up deleting the entire segment.
            if (prevMax != null && ORDERING.compare(seg.getMin(), prevMax) <= 0) {
                if (ORDERING.compare(prevMax, max) < 0) {
                    moveRecords(seg, prevMax, true);
                } else {
                    moveRecords(seg, max, false);
                    deleteSegment(seg);
                    continue;
                }
            }

            // The segment is ready for use.  Add it to the internal data structures.
            _segmentMap.put(seg.getMin(), seg);
            if (seg.isSplitting()) {
                _splitQueue.offer(seg);
            }

            prevMax = max;
        }
    }

    /** Move/copy segment records from a segment that isn't in segmentMap to the segments that are. */
    private void moveRecords(Segment seg, ByteBuffer max, boolean deleteFromSource) {
        checkWritesAllowed();
        ByteBuffer from = seg.getMin();
        ByteBuffer toExclusive = successor(max);

        int batchSize = scanBatchSize();
        Iterator<List<ByteBuffer>> batchIter = Iterators.partition(
                _dao.scanRecords(seg.getDataId(), from, toExclusive, batchSize, Integer.MAX_VALUE),
                batchSize);
        while (batchIter.hasNext()) {
            List<ByteBuffer> records = batchIter.next();

            // Write to the destination.  Go through addAll() to update stats, do splitting, etc.
            addAll(records);

            // Delete individual records from the source.
            if (deleteFromSource) {
                _dao.prepareUpdate(_name).deleteRecords(seg.getDataId(), records).execute();
            }
        }

        seg.setMin(toExclusive);
    }

    private void deleteSegment(Segment seg) {
        checkWritesAllowed();
        _dao.prepareUpdate(_name).deleteSegment(seg.getId(), seg.getDataId()).execute();
    }

    private int scanBatchSize() {
        return 1000;  // TODO
    }

    private <V> V valueOrNull(Map.Entry<?, V> entry) {
        return entry != null ? entry.getValue() : null;
    }

    private <T> Set<T> newBoundedHashSet(final int maxSize) {
        return Collections.newSetFromMap(new LinkedHashMap<T, Boolean>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<T, Boolean> eldest) {
                return size() > maxSize;
            }
        });
    }

    private int sumLengths(Collection<ByteBuffer> records) {
        int sum = 0;
        for (ByteBuffer record : records) {
            sum += record.remaining();
        }
        return sum;
    }

    /** Returns a ByteBuffer that, w/unsigned comparison, sorts immediately after the specified {@code buf}. */
    private ByteBuffer successor(@Nullable ByteBuffer buf) {
        if (buf == null) {
            return ByteBuffer.allocate(0);
        }
        ByteBuffer next = ByteBuffer.allocate(buf.remaining() + 1);
        next.put(buf.duplicate());
        next.put((byte) 0);
        next.flip();
        return next;
    }

    private Segment firstSegment() {
        return valueOrNull(_segmentMap.firstEntry());
    }

    private Segment ceilingSegment(ByteBuffer record) {
        Segment segment = valueOrNull(_segmentMap.ceilingEntry(record));
        return segment != null ? segment : firstSegment();  // wrap around if necessary
    }

    private Segment newSegment(@Nullable ByteBuffer min) {
        return new Segment(newUuid(), min, _splitThresholdBytes, _splitQueue);
    }

    private Segment newSegmentFromSnapshot(UUID id, String snapshotString) {
        Segment.Snapshot snapshot = JsonHelper.fromJson(snapshotString, Segment.Snapshot.class);
        return new Segment(id, snapshot, _splitThresholdBytes, _splitQueue);
    }

    private UUID newUuid() {
        if (!_uuidsToRecycle.isEmpty()) {
            return Iterators.consumingIterator(_uuidsToRecycle.iterator()).next();
        } else {
            return TimeUUIDs.newUUID();
        }
    }

    private class SegmentMods {
        private final List<SegmentMod> _mods = Lists.newArrayList();
        private final List<Runnable> _extras = Lists.newArrayList();

        SegmentMod with(Segment segment) {
            if (_mods.isEmpty() || _mods.get(_mods.size() - 1).getSegment() != segment) {
                _mods.add(new SegmentMod(segment));
            }
            return _mods.get(_mods.size() - 1);
        }

        void runAtCommit(Runnable extra) {
            _extras.add(extra);
        }

        /** Write changes to disk. */
        void commit() {
            if (_mods.isEmpty() && _dirtyList.isEmpty()) {
                return; // Nothing to do
            }

            UpdateRequest update = _dao.prepareUpdate(_name);
            List<UUID> uuidsToRecycle = Lists.newArrayList();

            // Apply queued updates.  This may add segments to the dirty list.
            for (SegmentMod mod : _mods) {
                mod.persist(update);
            }
            // Run custom steps after updates have been applied in-memory but before writing to disk.
            for (Runnable extra : _extras) {
                extra.run();
            }
            // Flush the list of dirty segments.
            for (Segment seg : _dirtyList) {
                persist(seg, update, uuidsToRecycle);
            }
            checkWritesAllowed();
            update.execute();

            // Success!
            _uuidsToRecycle.addAll(uuidsToRecycle);
            _dirtyList.clear();
            _mods.clear();
        }

        private void persist(Segment seg, UpdateRequest update, List<UUID> uuidsToRecycle) {
            if (seg.isDeleted()) {
                update.deleteSegment(seg.getId(), seg.getDataId());
                uuidsToRecycle.add(seg.getId());
            } else {
                update.writeSegment(seg.getId(), JsonHelper.asJson(seg.snapshot()));
            }
        }
    }

    private class SegmentMod {
        private final Segment _segment;
        private boolean _create;
        private boolean _createProvisionally;
        private boolean _setMin;
        private ByteBuffer _min;
        private final List<ByteBuffer> _recordAdds = Lists.newArrayList();
        private final List<ByteBuffer> _recordDeletes = Lists.newArrayList();

        SegmentMod(Segment segment) {
            _segment = segment;
        }

        Segment getSegment() {
            return _segment;
        }

        private SegmentMod create() {
            _create = true;
            return this;
        }

        /**
         * This will cause the segment to be persisted at commit time, but it won't be added to the segment map
         * until the returned {@code Runnable} is invoked.  This allows fine-grained control over the sequence
         * of updates to handle specific failure scenarios during a split operation.
         */
        Runnable createProvisionally() {
            _createProvisionally = true;
            return new Runnable() {
                @Override
                public void run() {
                    segmentMapPut(_segment);
                }
            };
        }

        SegmentMod delete() {
            setMin(MAX);
            return this;
        }

        SegmentMod setMin(ByteBuffer min) {
            _setMin = true;
            _min = min;
            return this;
        }

        SegmentMod write(ByteBuffer record) {
            _recordAdds.add(record);
            return this;
        }

        SegmentMod delete(ByteBuffer record) {
            _recordDeletes.add(record);
            return this;
        }

        private boolean isDelete() {
            return _setMin && _min == MAX;
        }

        void persist(UpdateRequest update) {
            Segment seg = _segment;

            // This will almost definitely change the in-memory Segment so persist the segment snapshot.
            _dirtyList.add(seg);

            if (isDelete()) {
                // Setting a segment's min to MAX will delete it.
                segmentMapRemove(seg);
                seg.onSegmentDeleted();

                // Ensure that read position is always left in a valid state.
                if (_readPosition == seg) {
                    _readPosition = ceilingSegment(seg.getMin());
                }
            } else {
                if (_create) {
                    segmentMapPut(seg);
                }

                // We only change the min by increasing it after deleting records.  By the time this method
                // is called we know the records-to-be-deleted have been consumed and are no longer necessary,
                // so we can safely move the min and not look at those deleted records again, even if the
                // actual DAO update to delete the records fails.
                if (_setMin) {
                    segmentMapRemove(seg);
                    seg.setMin(_min);
                    segmentMapPut(seg);
                }

                // Record deletes
                update.deleteRecords(seg.getDataId(), _recordDeletes);
                seg.onRecordsDeleted(_recordDeletes.size());

                // Record writes
                update.writeRecords(seg.getDataId(), _recordAdds);
                for (ByteBuffer record : _recordAdds) {
                    seg.onRecordAdded(record);
                }
            }
        }

        private void segmentMapRemove(Segment seg) {
            Segment old = _segmentMap.remove(seg.getMin());
            checkState(old == seg);
            updateEmpty();
        }

        private void segmentMapPut(Segment seg) {
            Segment old = _segmentMap.put(seg.getMin(), seg);
            checkState(old == null);
            updateEmpty();
        }

        // For debugging
        @Override
        public String toString() {
            Segment seg = _segment;
            StringBuilder buf = new StringBuilder();
            buf.append("SegmentMod[");
            buf.append("min=").append(seg.getMin() != null ? ByteBufferUtil.bytesToHex(seg.getMin()) : null);
            if (isDelete()) {
                buf.append(",delete");
            } else {
                if (_create) {
                    buf.append(",create");
                } else if (_createProvisionally) {
                    buf.append(",createProvisionally");
                }
                if (_setMin) {
                    buf.append(",move=").append(ByteBufferUtil.bytesToHex(_min));
                }
                if (!_recordAdds.isEmpty()) {
                    buf.append(",#add=").append(_recordAdds.size());
                }
                if (!_recordDeletes.isEmpty()) {
                    buf.append(",#delete=").append(_recordDeletes.size());
                }
            }
            buf.append("]");
            return buf.toString();
        }
    }

    private static class Budget {
        private long _budget;

        synchronized void clear() {
            _budget = 0;
        }

        synchronized void credit(long delta) {
            _budget += delta;
        }

        synchronized boolean debitIfAvailable(long delta) {
            if (_budget >= delta) {
                _budget -= delta;
                return true;
            }
            return false;
        }
    }
}
