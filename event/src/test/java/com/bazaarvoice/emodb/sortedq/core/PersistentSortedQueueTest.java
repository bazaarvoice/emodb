package com.bazaarvoice.emodb.sortedq.core;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.sortedq.api.Consumer;
import com.bazaarvoice.emodb.sortedq.api.SortedQueue;
import com.bazaarvoice.emodb.sortedq.db.QueueDAO;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Objects;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedInteger;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class PersistentSortedQueueTest {
    private static final ByteBufferOrdering ORDERING = ByteBufferOrdering.INSTANCE;

    private static Random RANDOM = initRandom(System.currentTimeMillis());

    /** If a test fails, take the seed printed below and add "initRandom(seed)" at the top of the test case. */
    private static Random initRandom(long seed) {
        System.out.printf("%s using random seed: %d%n", PersistentSortedQueueTest.class.getSimpleName(), seed);
        Random random = new Random(seed);
        PersistentSortedQueue.RANDOM = random;
        PersistentSortedQueueTest.RANDOM = random;
        return random;
    }

    @Test
    public void testWriteThenRead() {
        // Add 100k random 2-byte buffers to the queue.  Typically this generates ~50k distinct values.
        int n = 100000;
        int batchSize = 100;

        InMemoryQueueDAO dao = new InMemoryQueueDAO();
        SortedQueue q = new PersistentSortedQueue("queue", dao, new MetricRegistry());
        TreeSet<ByteBuffer> expected = Sets.newTreeSet(ORDERING);
        addBuffers(q, expected, n, batchSize, randomBufferIter(2));

        // Verify the size() method returns something within range.
        int distinct = expected.size();
        assertWithinRange(q.sizeEstimate(), distinct, 0.2, "Size");

        // Verify no splitting occurred.
        assertTrue(dao.getRecords().keySet().size() == 1);
        assertEquals(dao.getNumSegmentWrites(), n / batchSize);
        assertEquals(dao.getNumRecordWrites(), n);
        assertEquals(dao.getNumRecordDeletes(), 0);

        // Verify that scan returns the expected items in the same order.
        assertEquals(q.scan(null, Long.MAX_VALUE), expected.iterator());

        // Verify that drainTo returns the expected items in the same order.
        assertDrain(q, expected, Long.MAX_VALUE);

        assertFalse(q.scan(null, Long.MAX_VALUE).hasNext());

        assertEquals(dao.getNumSegmentDeletes(), 1);
        assertTrue(dao.getRecords().isEmpty());
    }

    @Test
    public void testWriteThenReadWithSplitting() {
        // Add 100k random 2-byte buffers to the queue.  Typically this generates ~50k distinct values.
        // Reduce the splitting thresholds so that splitting occurs.
        int n = 100000;
        int splitThresholdBytes = n / 10;

        InMemoryQueueDAO dao = new InMemoryQueueDAO();
        SortedQueue q = new PersistentSortedQueue("queue", false, splitThresholdBytes, splitThresholdBytes / 10, dao, new MetricRegistry());
        TreeSet<ByteBuffer> expected = Sets.newTreeSet(ORDERING);
        addBuffers(q, expected, n, 100, randomBufferIter(2));

        // Verify the size() method returns something within range.
        assertWithinRange(q.sizeEstimate(), expected.size(), 0.2, "Size");

        // Verify splitting successfully kept the row sizes manageable.
        assertSmallRows(dao, splitThresholdBytes * 2);

        // Verify splitting cost was reasonable.  For random traffic, 2*n is a generous bound, typically it's < 1.5*n.
        assertTrue(dao.getNumRecordWrites() <= 2 * n, "Splitting caused too many writes: " + dao.getNumRecordWrites());
        assertTrue(dao.getNumRecordDeletes() <= 2 * n, "Splitting caused too many deletes: " + dao.getNumRecordDeletes());

        // Verify that scan returns the expected items in the same order.
        assertEquals(q.scan(null, Long.MAX_VALUE), expected.iterator());

        // Verify that drainTo returns the expected items in the same order.
        assertDrain(q, expected, Long.MAX_VALUE);

        assertFalse(q.scan(null, Long.MAX_VALUE).hasNext());
    }

    @Test
    public void testMixedReadAndWriteWithSplitting() {
        // Reduce the splitting thresholds so that splitting occurs.
        QueueDAO dao = new InMemoryQueueDAO();
        SortedQueue q = new PersistentSortedQueue("queue", false, 1000, 100, dao, new MetricRegistry());
        TreeSet<ByteBuffer> expected = Sets.newTreeSet(ORDERING);

        // Write and read in batches.  Don't verify order since order isn't 100% deterministic when
        // splitting occurs, but do verify no data is lost and no phantom data appears.
        for (int[] constants : new int[][]{{300, 100}, {100, 300}}) {
            for (int i = 0; i < 500; i++) {
                int numWrite = 1 + RANDOM.nextInt(constants[0]);
                int numRead = 1 + RANDOM.nextInt(constants[1]);

                addBuffers(q, expected, numWrite, 100, randomBufferIter(2));

                assertDrain(q, expected, numRead);
            }
        }

        // Verify that scan returns the expected items in the same order.
        assertEquals(q.scan(null, Long.MAX_VALUE), expected.iterator());

        // Verify that drainTo returns the expected items in the same order.
        assertDrain(q, expected, Long.MAX_VALUE);

        assertFalse(q.scan(null, Long.MAX_VALUE).hasNext());
    }

    /**
     * Same as testMixedReadAndWriteWithSplitting but simulates frequent persistence errors to ensure that
     * the queue still produces reasonable results.  This version retries operations until they succeed,
     * verifying that the in-memory and on-disk data structures don't get wildly out-of-sync.
     */
    @Test
    public void testMixedReadAndWriteWithRetryAfterCrash() {
        // Configure writes to fail 25% of the time.
        InMemoryQueueDAO dao = new InMemoryQueueDAO(newCrashDecider(0.25), RANDOM);

        PersistentSortedQueueWithRetry q = new PersistentSortedQueueWithRetry(dao, "queue", 1000, 100, false, new MetricRegistry());
        TreeSet<ByteBuffer> expected = Sets.newTreeSet(ORDERING);
        Set<ByteBuffer> all = Sets.newHashSet();

        // Write and read in batches.  Don't verify order since order isn't 100% deterministic when splitting
        // occurs, but do verify no data is lost.  Because failures can cause deletes to get lost we may every
        // so often get a duplicate event--the drainTo method will produce a record that has already been
        // produced.  So relax the correctness constraints relative to testMixedReadAndWriteWithSplitting().
        for (int[] constants : new int[][]{{300, 100}, {100, 300}}) {
            for (int i = 0; i < 500; i++) {
                int numWrite = 1 + RANDOM.nextInt(constants[0]);
                int numRead = 1 + RANDOM.nextInt(constants[1]);

                addBuffers(q, expected, all, numWrite, 100, randomBufferIter(2));

                assertDrain(q, expected, all, numRead);
            }
        }

        // Verify that drainTo returns the expected items in the same order.
        assertDrain(q, expected, all, Long.MAX_VALUE);

        // Some exceptions should have been thrown...
        assertTrue(q.getNumFailures() > 0);
    }

    /**
     * Same as testMixedReadAndWriteWithSplitting but simulates frequent persistence errors to ensure that
     * the queue still produces reasonable results.  This version throws away the in-memory state and reloads
     * it from disk after every crash, which is extreme and causes lots of duplicate events.
     */
    @Test
    public void testMixedReadAndWriteWithReloadAfterCrash() {
        // Configure writes to fail 25% of the time.
        InMemoryQueueDAO dao = new InMemoryQueueDAO(newCrashDecider(0.25), RANDOM);

        PersistentSortedQueueWithRetry q = new PersistentSortedQueueWithRetry(dao, "queue", 1000, 100, true, new MetricRegistry());
        TreeSet<ByteBuffer> expected = Sets.newTreeSet(ORDERING);
        Set<ByteBuffer> all = Sets.newHashSet();

        // Write and read in batches.  Don't verify order since order isn't 100% deterministic when splitting
        // occurs, but do verify no data is lost.  Because failures can cause deletes to get lost we may every
        // so often get a duplicate event--the drainTo method will produce a record that has already been
        // produced.  So relax the correctness constraints relative to testMixedReadAndWriteWithSplitting().
        for (int[] constants : new int[][]{{300, 100}, {100, 300}}) {
            for (int i = 0; i < 500; i++) {
                int numWrite = 1 + RANDOM.nextInt(constants[0]);
                int numRead = 1 + RANDOM.nextInt(constants[1]);

                addBuffers(q, expected, all, numWrite, 100, randomBufferIter(2));

                assertDrain(q, expected, all, numRead);
            }
        }

        // Verify that drainTo returns the expected items in the same order.
        assertDrain(q, expected, all, Long.MAX_VALUE);

        // Some exceptions should have been thrown...
        assertTrue(q.getNumFailures() > 0);
    }

    @Test
    public void testSplittingWithAscendingWrites() {
        InMemoryQueueDAO dao = new InMemoryQueueDAO();

        // Reduce the splitting thresholds so that splitting occurs.
        SortedQueue q = new PersistentSortedQueue("queue", false, 10000, 1000, dao, new MetricRegistry());
        TreeSet<ByteBuffer> expected = Sets.newTreeSet(ORDERING);

        int n = 100000;
        addBuffers(q, expected, n, 100, sequentialBufferIter(0, 1));

        // Verify splitting successfully kept the row sizes manageable.
        assertSmallRows(dao, 2000);

        // Verify splitting cost was reasonable.  Max #writes should be 3*n, but for sequential writes it's ~2*n in practice.
        assertTrue(dao.getNumRecordWrites() <= 3 * n, "Splitting caused too many writes: " + dao.getNumRecordWrites());
        assertTrue(dao.getNumRecordDeletes() <= 2 * n, "Splitting caused too many deletes: " + dao.getNumRecordDeletes());

        // Verify that drainTo returns the expected items in the same order.
        assertDrain(q, expected, Long.MAX_VALUE);
    }

    @Test
    public void testSplittingWithDescendingWrites() {
        // Reduce the splitting thresholds so that splitting occurs.
        InMemoryQueueDAO dao = new InMemoryQueueDAO();
        SortedQueue q = new PersistentSortedQueue("queue", false, 10000, 1000, dao, new MetricRegistry());
        TreeSet<ByteBuffer> expected = Sets.newTreeSet(ORDERING);

        int n = 100000;
        addBuffers(q, expected, n, 100, sequentialBufferIter(UnsignedInteger.MAX_VALUE.intValue(), -1));

        // Verify splitting successfully kept the row sizes manageable.
        assertSmallRows(dao, 2000);

        // Verify splitting cost was reasonable.  Max #writes should be 3*n, but for sequential writes it's ~2*n in practice.
        assertTrue(dao.getNumRecordWrites() <= 3 * n, "Splitting caused too many writes: " + dao.getNumRecordWrites());
        assertTrue(dao.getNumRecordDeletes() <= 2 * n, "Splitting caused too many deletes: " + dao.getNumRecordDeletes());

        // Verify that drainTo returns the expected items in the same order.
        assertDrain(q, expected, Long.MAX_VALUE);
    }

    @Test
    public void testReadOrder() {
        SortedQueue q = new PersistentSortedQueue("queue", new InMemoryQueueDAO(), new MetricRegistry());

        // Write values 1, 2, 3
        q.addAll(ImmutableList.of(intBuffer(1), intBuffer(2), intBuffer(3)));

        // Read 1, leaving the read position pointing at 2.
        q.drainTo(new Consumer() {
            @Override
            public void consume(List<ByteBuffer> records) {
                assertEquals(records, ImmutableList.of(intBuffer(1)));
            }
        }, 1);

        // Write value 0, creating a new segment.
        q.addAll(ImmutableList.of(intBuffer(0)));

        // Read 2, leaving the read position pointing at 3.  This verifies a bug where it would incorrectly
        // wrap around and leave the read position pointing at 0.
        q.drainTo(new Consumer() {
            @Override
            public void consume(List<ByteBuffer> records) {
                assertEquals(records, ImmutableList.of(intBuffer(2)));
            }
        }, 1);

        // Read 3 and 0.
        q.drainTo(new Consumer() {
            @Override
            public void consume(List<ByteBuffer> records) {
                assertEquals(records, ImmutableList.of(intBuffer(3), intBuffer(0)));
            }
        }, Long.MAX_VALUE);
    }

    @Test
    public void testWritePreservesReadPosition() {
        SortedQueue q = new PersistentSortedQueue("queue", new InMemoryQueueDAO(), new MetricRegistry());

        q.addAll(ImmutableList.of(intBuffer(1), intBuffer(2)));

        // Read 1 value, leaving the read position pointing at 2.
        q.drainTo(new Consumer() {
            @Override
            public void consume(List<ByteBuffer> records) {
                assertEquals(records, ImmutableList.of(intBuffer(1)));
            }
        }, 1);

        // Write a few more values.  The read position should be left still pointing to 2.
        q.addAll(ImmutableList.of(intBuffer(0), intBuffer(1), intBuffer(3)));

        // Verify that 2 comes back first.
        q.drainTo(new Consumer() {
            @Override
            public void consume(List<ByteBuffer> records) {
                assertEquals(records, ImmutableList.of(intBuffer(2), intBuffer(3), intBuffer(0), intBuffer(1)));
            }
        }, Long.MAX_VALUE);
    }

    @Test
    public void testEstimatedSizeIsPositive() {
        SortedQueue q = new PersistentSortedQueue("queue", new InMemoryQueueDAO(), new MetricRegistry());
        TreeSet<ByteBuffer> expected = Sets.newTreeSet(ORDERING);

        // Add enough values that the cardinality estimator must estimate.
        int n = 10000;
        addBuffers(q, expected, n, n, sequentialBufferIter(0, 1));

        // Remove all but one.
        assertDrain(q, expected, n - 1);
        assertEquals(expected.size(), 1);
        assertTrue(q.sizeEstimate() >= 1);
        assertTrue(!q.isEmpty());

        assertDrain(q, expected, 1);
        assertEquals(expected.size(), 0);
        assertEquals(q.sizeEstimate(), 0);
        assertTrue(q.isEmpty());
    }

    @Test
    public void testScan() {
        InMemoryQueueDAO dao = new InMemoryQueueDAO();
        SortedQueue q = new PersistentSortedQueue("queue", false, 1000, 250, dao, new MetricRegistry());
        TreeSet<ByteBuffer> expected = Sets.newTreeSet(ORDERING);

        addBuffers(q, expected, 1000, 100, sequentialBufferIter(0, 1));

        // Make sure we got some splitting or else the rest of the test isn't very interesting
        assertTrue(dao.getRecords().keySet().size() > 1);

        // Verify that a full scan gets everything.
        assertEquals(q.scan(null, Long.MAX_VALUE), expected.iterator());

        // Try with different start values and a non-infinite limit, verify results are as expected.
        for (int i = 0; i < 1100; i += 60) {
            ByteBuffer start = intBuffer(i);
            assertEquals(q.scan(start, 100), Iterators.limit(expected.tailSet(start).iterator(), 100));
        }

        // Try a scan that should return zero results.
        assertFalse(q.scan(intBuffer(9999), Long.MAX_VALUE).hasNext());
    }

    @Test
    public void testClear() {
        InMemoryQueueDAO dao = new InMemoryQueueDAO();
        SortedQueue q = new PersistentSortedQueue("queue", dao, new MetricRegistry());

        q.addAll(sequentialBuffers(0, 1000, 1));
        assertFalse(q.isEmpty());

        q.clear();
        assertTrue(q.isEmpty());

        assertEquals(dao.getNumSegmentDeletes(), 1);
        assertEquals(dao.getNumRecordDeletes(), 0);
    }

    @Test
    public void testLoadReadOnly() {
        InMemoryQueueDAO dao = new InMemoryQueueDAO();
        SortedQueue wq = new PersistentSortedQueue("my-queue", dao, new MetricRegistry());
        TreeSet<ByteBuffer> expected = Sets.newTreeSet(ORDERING);
        addBuffers(wq, expected, 10, 10, randomBufferIter(2));

        // Create a new read-only queue that can be used to inspect "wq".
        SortedQueue q = new PersistentSortedQueue("my-queue", true, dao, new MetricRegistry());
        assertFalse(q.isEmpty());
        assertTrue(q.sizeEstimate() > 0);
        assertTrue(q.sizeEstimate() < 20);
        assertEquals(Sets.newHashSet(q.scan(null, Long.MAX_VALUE)), expected);

        // Verify that operations that write are not allowed.
        try {
            q.clear();
            fail();
        } catch (ReadOnlyQueueException e) {
            assertEquals(e.getMessage(), "Queue has been marked read-only, writes are not allowed: my-queue");
        }
        try {
            q.addAll(sequentialBuffers(0, 10, 1));
            fail();
        } catch (ReadOnlyQueueException e) {
            assertEquals(e.getMessage(), "Queue has been marked read-only, writes are not allowed: my-queue");
        }
        try {
            q.drainTo(mock(Consumer.class), Long.MAX_VALUE);
            fail();
        } catch (ReadOnlyQueueException e) {
            assertEquals(e.getMessage(), "Queue has been marked read-only, writes are not allowed: my-queue");
        }

        // Flip the original "wq" to read-only and verify that it now can't be modified.
        assertFalse(wq.isReadOnly());
        wq.setReadOnly();
        assertTrue(wq.isReadOnly());
        try {
            wq.addAll(sequentialBuffers(0, 10, 1));
            fail();
        } catch (ReadOnlyQueueException e) {
            assertEquals(e.getMessage(), "Queue has been marked read-only, writes are not allowed: my-queue");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testFixSegmentOverlapOnLoad() {
        InMemoryQueueDAO dao = new InMemoryQueueDAO();
        SplitQueue<Segment> splitQueue = mock(SplitQueue.class);

        // Create in reverse order so the last one created wins when there are ties.
        Segment seg4 = new Segment(TimeUUIDs.newUUID(), (ByteBuffer) null, 1000, splitQueue);
        Segment seg3 = new Segment(TimeUUIDs.newUUID(), (ByteBuffer) null, 1000, splitQueue);
        Segment seg2 = new Segment(TimeUUIDs.newUUID(), (ByteBuffer) null, 1000, splitQueue);
        Segment seg1 = new Segment(TimeUUIDs.newUUID(), (ByteBuffer) null, 1000, splitQueue);

        // Write two segments with records that overlap in range.  A 3rd segment is subsumed, a 4th has no records
        List<ByteBuffer> records1 = sequentialBuffers(0, 10000, 2);  // evens 0..19998
        List<ByteBuffer> records2 = Lists.newArrayList(sequentialBuffers(1, 10000, 2));  // odds 1..19999
        records2.addAll(ImmutableList.of(intBuffer(0), intBuffer(2), intBuffer(19998))); // add some evens shared w/record1
        List<ByteBuffer> records3 = sequentialBuffers(19997, 3, 1);  // 19997..19999
        dao.prepareUpdate("queue")
                .writeSegment(seg1.getId(), JsonHelper.asJson(seg1.snapshot()))
                .writeSegment(seg2.getId(), JsonHelper.asJson(seg2.snapshot()))
                .writeSegment(seg3.getId(), JsonHelper.asJson(seg3.snapshot()))
                .writeSegment(seg4.getId(), JsonHelper.asJson(seg4.snapshot()))
                .writeRecords(seg1.getDataId(), records1)
                .writeRecords(seg2.getDataId(), records2)
                .writeRecords(seg3.getDataId(), records3)
                .execute();

        // Loading the queue should detect and eliminate the overlap.
        SortedQueue q = new PersistentSortedQueue("queue", dao, new MetricRegistry());

        // Verify that records were moved to the expected places.
        List<ByteBuffer> actual1 = ImmutableList.copyOf(dao.scanRecords(seg1.getDataId(), null, null, 1000, Integer.MAX_VALUE));
        List<ByteBuffer> actual2 = ImmutableList.copyOf(dao.scanRecords(seg2.getDataId(), null, null, 1000, Integer.MAX_VALUE));
        List<ByteBuffer> actual3 = ImmutableList.copyOf(dao.scanRecords(seg3.getDataId(), null, null, 1000, Integer.MAX_VALUE));
        List<ByteBuffer> actual4 = ImmutableList.copyOf(dao.scanRecords(seg4.getDataId(), null, null, 1000, Integer.MAX_VALUE));
        assertEquals(actual1, sequentialBuffers(0, 19999, 1));  // seg1 is shorter and gets most of the records
        assertEquals(actual2, sequentialBuffers(19999, 1, 1));  // leaving seg2 with just one last element
        assertEquals(actual3, Collections.emptyList());
        assertEquals(actual4, Collections.emptyList());

        // Verify that the segment metadata was updated.
        Map<UUID, String> segments = dao.loadSegments("queue");
        assertEquals(JsonHelper.fromJson(segments.get(seg1.getId()), Segment.Snapshot.class).min, null);
        assertTrue(segments.containsKey(seg2.getId()));
        assertFalse(segments.containsKey(seg3.getId()));
        assertFalse(segments.containsKey(seg4.getId()));

        // Verify that no records were lost, the queue returns them all.
        TreeSet<ByteBuffer> expected = Sets.newTreeSet(ORDERING);
        expected.addAll(records1);
        expected.addAll(records2);
        expected.addAll(records3);
        assertDrain(q, expected, Integer.MAX_VALUE);
    }

    @Test
    public void testSegmentUuidTombstones() {
        InMemoryQueueDAO dao = new InMemoryQueueDAO();
        SortedQueue q = new PersistentSortedQueue("my-queue", dao, new MetricRegistry());
        Set<UUID> uniqueSegmentIds = Sets.newHashSet();
        Map<UUID, String> daoSegments = dao.getManifest().row("my-queue");

        // To get good coverage on the algorithm, use a data stream that isn't monotonically increasing or decreasing.
        Iterator<ByteBuffer> source = zip(sequentialBufferIter(0, 1), sequentialBufferIter(-1, -1));

        // Loop 1: each iteration drains the entire queue
        for (int i = 0; i < 100; i++) {
            q.addAll(consume(source, 10));
            uniqueSegmentIds.addAll(daoSegments.keySet());
            q.drainTo(mock(Consumer.class), Long.MAX_VALUE);
            assertTrue(q.isEmpty());
        }

        // Loop 2: each iteration clears the queue w/o reading individual events
        for (int i = 0; i < 100; i++) {
            q.addAll(consume(source, 10));
            uniqueSegmentIds.addAll(daoSegments.keySet());
            q.clear();
            assertTrue(q.isEmpty());
        }

        // Loop 3: each iteration leaves some data in the queue
        q.addAll(consume(source, 5));
        for (int i = 0; i < 100; i++) {
            q.addAll(consume(source, 10));
            uniqueSegmentIds.addAll(daoSegments.keySet());
            q.drainTo(mock(Consumer.class), 10);
            assertFalse(q.isEmpty());
        }
        q.clear();

        // Loop 4: each iteration is designed to ensure at least 2 segments are active at some point
        for (int i = 0; i < 100; i++) {
            // Add 10, remove 5, add 5 more (now there should be 2 segments), drain 10 to clear the queue.
            q.addAll(consume(source, 10));
            uniqueSegmentIds.addAll(daoSegments.keySet());
            q.drainTo(mock(Consumer.class), 5);
            q.addAll(consume(source, 5));
            uniqueSegmentIds.addAll(daoSegments.keySet());
            q.drainTo(mock(Consumer.class), 10);
            assertTrue(q.isEmpty());
        }

        assertTrue(uniqueSegmentIds.size() >= 1, "Test didn't measure anything.");
        assertTrue(uniqueSegmentIds.size() <= 5,
                "Used too many segment UUIDs, Cassandra tombstones will hurt performance: " + uniqueSegmentIds.size());
    }

    @Test
    public void testDrainToTimeout() throws Exception {
        InMemoryQueueDAO dao = new InMemoryQueueDAO();
        final SortedQueue q = new PersistentSortedQueue("my-queue", dao, new MetricRegistry());
        ExecutorService threadPool = Executors.newCachedThreadPool();

        // Seed the queue with some data
        q.addAll(sequentialBuffers(0, 5000, 1));

        // Start one thread that gets into drainTo() and does something that takes a while.
        final CountDownLatch latch = new CountDownLatch(1);
        Future<Void> thread1 = threadPool.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                q.drainTo(new Consumer() {
                    @Override
                    public void consume(List<ByteBuffer> records) {
                        assertEquals(latch.getCount(), 1);
                        latch.countDown();
                        assertTrue(records.size() < 5000, "Didn't expect to get all in one batch: " + records.size());
                        // Sleep for longer than the configured timeout, ensuring that consume isn't called again.
                        sleep(300);
                    }
                }, Long.MAX_VALUE, Duration.ofMillis(250));
                return null;
            }
        });
        // Start a second thread that contends for the drainTo() lock but doesn't get it.
        Future<Object> thread2 = threadPool.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                // Wait for the first thread enters drainTo and obtains the lock
                assertTrue(latch.await(1, TimeUnit.SECONDS));
                // The drainTo() call should timeout acquiring the lock and do nothing.
                q.drainTo(new Consumer() {
                    @Override
                    public void consume(List<ByteBuffer> records) {
                        fail();
                    }
                }, Long.MAX_VALUE, Duration.ofMillis(50));
                return null;
            }
        });
        // Make sure both threads succeeded w/o throwing exceptions
        thread1.get();
        thread2.get();
        threadPool.shutdown();
    }

    private void addBuffers(SortedQueue actual, Collection<ByteBuffer> expected,
                            int n, int batchSize, Iterator<ByteBuffer> generatorIter) {
        addBuffers(actual, expected, Lists.<ByteBuffer>newArrayList(), n, batchSize, generatorIter);
    }

    private void addBuffers(SortedQueue actual, Collection<ByteBuffer> expected, Collection<ByteBuffer> all,
                            int n, int batchSize, Iterator<ByteBuffer> generatorIter) {
        Iterator<List<ByteBuffer>> batchIter =
                Iterators.partition(Iterators.limit(generatorIter, n), batchSize);
        while (batchIter.hasNext()) {
            List<ByteBuffer> batch = batchIter.next();
            actual.addAll(batch);
            expected.addAll(batch);
            all.addAll(batch);
        }
    }

    private void assertDrain(SortedQueue actual, final NavigableSet<ByteBuffer> expected, long limit) {
        assertDrain(actual, expected, Collections.<ByteBuffer>emptySet(), limit);
    }

    private void assertDrain(SortedQueue actual, final NavigableSet<ByteBuffer> expected,
                             final Set<ByteBuffer> allowedPhantoms, long limit) {
        final AtomicInteger count = new AtomicInteger();
        actual.drainTo(new Consumer() {
            @Override
            public void consume(List<ByteBuffer> records) {
                ByteBuffer first = records.get(0);
                boolean wrapped = false;

                PeekingIterator<ByteBuffer> expectedIter =
                        Iterators.peekingIterator(expected.tailSet(first, true).iterator());
                for (ByteBuffer actual : records) {
                    count.incrementAndGet();

                    if (!expectedIter.hasNext() && !wrapped) {
                        expectedIter = Iterators.peekingIterator(expected.headSet(first, false).iterator());
                        wrapped = true;
                    }

                    if (!expectedIter.hasNext()) {
                        if (allowedPhantoms.contains(actual)) {
                            continue;
                        }
                        fail("Unexpected entry in sorted queue: " + ByteBufferUtil.bytesToHex(actual));
                    }

                    ByteBuffer expected = expectedIter.peek();

                    if (!Objects.equal(actual, expected)) {
                        if (allowedPhantoms.contains(actual)) {
                            continue;
                        }
                        fail(String.format("actual=%s, expected=%s", ByteBufferUtil.bytesToHex(actual), ByteBufferUtil.bytesToHex(expected)));
                    }

                    // If we matched successfully, advance the "expected" iterator.
                    expectedIter.next();
                    expectedIter.remove();
                }
            }
        }, limit);

        assertTrue(count.get() <= limit, "Drain returned more items than limit: " + count + " >= " + limit);

        // The queue should always be empty after it's fully drained.
        if (count.get() < limit) {
            assertEquals(actual.sizeEstimate(), 0);
            assertTrue(actual.isEmpty());
            assertTrue(expected.isEmpty());
        }
    }

    private void assertWithinRange(long actual, long expected, double expectedError, String name) {
        double actualError = Math.abs(expected - actual) / (double) expected;
        assertTrue(actualError <= expectedError, String.format(
                "%s error should be at most %g%%: %g (actual=%,d, expected=%,d)",
                name, expectedError * 100, actualError, actual, expected));
    }

    private void assertSmallRows(InMemoryQueueDAO dao, int max) {
        for (Collection<ByteBuffer> row : dao.getRecords().asMap().values()) {
            assertTrue(row.size() <= max * 2, "Row is too large: " + row.size());
        }
    }

    private Supplier<Boolean> newCrashDecider(final double p) {
        return new Supplier<Boolean>() {
            @Override
            public Boolean get() {
                return RANDOM.nextDouble() < p;
            }
        };
    }

    /** Zips two infinite iterators together, interleaving their elements. */
    private <T> Iterator<T> zip(final Iterator<T> iter1, final Iterator<T> iter2) {
        return new AbstractIterator<T>() {
            private boolean _toggle;
            @Override
            protected T computeNext() {
                _toggle = !_toggle;
                return (_toggle ? iter1 : iter2).next();
            }
        };
    }

    private ImmutableList<ByteBuffer> consume(Iterator<ByteBuffer> source, int n) {
        return ImmutableList.copyOf(Iterators.limit(source, n));
    }

    private List<ByteBuffer> sequentialBuffers(int start, int count, int delta) {
        return consume(sequentialBufferIter(start, delta), count);
    }

    /** Returns 4-byte buffers where each value is equal to the previous value + delta. */
    private Iterator<ByteBuffer> sequentialBufferIter(final int start, final int delta) {
        return new AbstractIterator<ByteBuffer>() {
            private int _current = start;

            @Override
            protected ByteBuffer computeNext() {
                try {
                    return intBuffer(_current);
                } finally {
                    _current += delta;
                }
            }
        };
    }

    private ByteBuffer intBuffer(int n) {
        ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(n);
        buf.flip();
        return buf;
    }

    private Iterator<ByteBuffer> randomBufferIter(final int bufferSize) {
        return new AbstractIterator<ByteBuffer>() {
            @Override
            protected ByteBuffer computeNext() {
                return randomBuffer(bufferSize);
            }
        };
    }

    private ByteBuffer randomBuffer(int bufferSize) {
        byte[] buf = new byte[bufferSize];
        RANDOM.nextBytes(buf);
        return ByteBuffer.wrap(buf);
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
