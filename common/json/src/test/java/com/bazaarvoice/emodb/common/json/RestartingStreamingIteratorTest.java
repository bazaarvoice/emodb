package com.bazaarvoice.emodb.common.json;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class RestartingStreamingIteratorTest {

    @Test
    public void testRestart() {
        int min = -50, max = 123, num = max - min, maxConsecutiveSuccessful = 10, limit = Integer.MAX_VALUE;

        MockStreamingIteratorSupplier supplier = new MockStreamingIteratorSupplier(min, max, maxConsecutiveSuccessful);
        int counter = min;
        for (String string : RestartingStreamingIterator.stream(null, limit, supplier)) {
            assertEquals(Integer.parseInt(string), counter++);
        }
        assertEquals(counter, max);

        List<MockIterator> iterators = supplier.getIterators();

        // Make sure we restarted the expected # of times.
        assertTrue(iterators.size() >= num / maxConsecutiveSuccessful);

        // All but the last iterator should have failed
        for (int i = 0; i < iterators.size() - 1; i++) {
            assertTrue(iterators.get(i).isFailed());
        }
        assertFalse(iterators.get(iterators.size() - 1).isFailed());

        // Check that all iterators were closed
        for (MockIterator iterator : iterators) {
            assertTrue(iterator.isClosed());
        }
    }

    @Test
    public void testRestartWithLimit() {
        int min = -50, max = Integer.MAX_VALUE, maxConsecutiveSuccessful = 10, limit = 123;

        MockStreamingIteratorSupplier supplier = new MockStreamingIteratorSupplier(min, max, maxConsecutiveSuccessful);
        int counter = min;
        for (String string : RestartingStreamingIterator.stream(null, limit, supplier)) {
            assertEquals(Integer.parseInt(string), counter++);
        }
        assertEquals(counter, min + limit);

        List<MockIterator> iterators = supplier.getIterators();

        // Make sure we restarted the expected # of times.
        assertTrue(iterators.size() >= limit / maxConsecutiveSuccessful);

        // All but the last iterator should have failed
        for (int i = 0; i < iterators.size() - 1; i++) {
            assertTrue(iterators.get(i).isFailed());
        }
        assertFalse(iterators.get(iterators.size() - 1).isFailed());

        // Check that all iterators were closed
        for (MockIterator iterator : iterators) {
            assertTrue(iterator.isClosed());
        }
    }

    /** Verify no retry when restarting the underlying iterator fails--the StreamingIteratorSupplier should handle retry. */
    @Test
    public void testFailureOnRestart() {
        final AtomicInteger restarts = new AtomicInteger();
        Iterator<String> iter = RestartingStreamingIterator.stream(null, Long.MAX_VALUE, new StreamingIteratorSupplier<String, Integer>() {
            @Override
            public Iterator<String> get(Integer fromToken, long limit) {
                if (fromToken == null) {
                    return new MockIterator(0, 5, true);
                } else {
                    assertEquals(fromToken.intValue(), 4);
                    restarts.incrementAndGet();
                    throw new RuntimeException("simulated-fail");
                }
            }

            @Override
            public Integer getNextToken(String object) {
                return Integer.parseInt(object);
            }
        }).iterator();

        // Verify the first few results are returned and handled correctly.
        for (int i = 0; i < 5; i++) {
            assertEquals(iter.next(), Integer.toString(i));
        }

        // Verify that the next item fails unrecoverably.
        try {
            iter.next();
            fail();
        } catch (RuntimeException e) {
            assertEquals(e.getMessage(), "simulated-fail");
            assertEquals(restarts.get(), 1);
        }
    }

    /** Verify no retry when restarting the underlying iterator fails--the StreamingIteratorSupplier should handle retry. */
    @Test
    public void testFailureImmediatelyAfterRestart() {
        final AtomicInteger restarts = new AtomicInteger();
        Iterator<String> iter = RestartingStreamingIterator.stream(null, Long.MAX_VALUE, new StreamingIteratorSupplier<String, Integer>() {
            @Override
            public Iterator<String> get(Integer fromToken, long limit) {
                if (fromToken == null) {
                    return new MockIterator(0, 5, true);
                } else {
                    // Return an iterator that fails immediately
                    restarts.incrementAndGet();
                    return new MockIterator(fromToken, fromToken, true);
                }
            }

            @Override
            public Integer getNextToken(String object) {
                return Integer.parseInt(object);
            }
        }).iterator();

        // Verify the first few results are returned and handled correctly.
        for (int i = 0; i < 5; i++) {
            assertEquals(iter.next(), Integer.toString(i));
        }

        // Verify that the next item fails unrecoverably.
        try {
            iter.next();
            fail();
        } catch (JsonStreamingEOFException e) {
            assertEquals(restarts.get(), 1);
        }
    }

    /** Returns a sequence of iterators that each fail w/in a specified # of elements. */
    private static class MockStreamingIteratorSupplier implements StreamingIteratorSupplier<String, Integer> {
        private final int _min;
        private final int _max;
        private final int _maxConsecutiveSuccessful;
        private final Random _rnd = new Random();
        private final List<MockIterator> _iterators = Lists.newArrayList();

        public MockStreamingIteratorSupplier(int min, int max, int maxConsecutiveSuccessful) {
            _min = min;
            _max = max;
            _maxConsecutiveSuccessful = maxConsecutiveSuccessful;
        }

        @Override
        public Iterator<String> get(Integer fromToken, long limit) {
            int start = fromToken == null ? _min : fromToken + 1;
            int end = start + 1 + _rnd.nextInt(_maxConsecutiveSuccessful - 1);
            MockIterator iter = new MockIterator(start, Math.min(end, _max), end <= _max);
            _iterators.add(iter);
            return iter;
        }

        @Override
        public Integer getNextToken(String string) {
            return Integer.parseInt(string);
        }

        public List<MockIterator> getIterators() {
            return _iterators;
        }
    }

    private static class MockIterator extends AbstractIterator<String> implements Closeable {
        private final int _end;
        private final boolean _fail;
        private int _next;
        private boolean _failed;
        private boolean _closed;

        public MockIterator(int start, int end, boolean fail) {
            _end = end;
            _fail = fail;
            _next = start;
        }

        @Override
        protected String computeNext() {
            if (_next == _end) {
                if (_fail) {
                    _failed = true;
                    throw new JsonStreamingEOFException();
                } else {
                    return endOfData();
                }
            }
            return String.valueOf(_next++);
        }

        public boolean isFailed() {
            return _failed;
        }

        public boolean isClosed() {
            return _closed;
        }

        @Override
        public void close() throws IOException {
            _closed = true;
        }
    }
}
