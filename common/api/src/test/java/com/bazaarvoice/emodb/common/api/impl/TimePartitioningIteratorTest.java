package com.bazaarvoice.emodb.common.api.impl;

import com.google.common.collect.Iterables;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TimePartitioningIteratorTest {
    @BeforeMethod
    public void setup() {
        // This test is sensitive to timing.  Do some warmup activity:
        // Prevent classloading from occurring while the test runs.
        Iterator<List<Integer>> iter = TimePartitioningIterator.partition(
                Iterables.cycle(1), 5, 5, 500, Duration.ofMillis(5)).iterator();
        for (int i = 0; i < 5000; i++) {
            iter.next();
        }
    }

    @Test
    public void test() {
        Iterable<Integer> forever = Iterables.cycle(1);

        int min = 5;
        int max = 500;
        Duration goal = Duration.ofMillis(35L);
        Iterator<List<Integer>> iter = TimePartitioningIterator.partition(forever, min, min, max, goal).iterator();

        // Give it a few iterations to determine the right partition size
        int size = 0;
        for (int i = 0; i < 20; i++) {
            size = iter.next().size();
            assertTrue(size >= min);
            assertTrue(size <= max);
            if (testInRange(size, goal.toMillis(), 3) == 0) break;
            sleep(size);
        }
        assertWithinRange(size, goal.toMillis(), 3);

        // Now slow things down and see that it adjusts and makes the partition size smaller
        for (int i = 0; i < 20; i++) {
            size = iter.next().size();
            assertTrue(size >= min);
            assertTrue(size <= max);
            if (testInRange(size, goal.toMillis() / 3, 3) == 0) break;
            sleep(size * 3);
        }
        assertWithinRange(size, goal.toMillis() / 3, 3);

        // Speed things up and see that it adjusts and makes the partition size bigger
        for (int i = 0; i < 20; i++) {
            size = iter.next().size();
            assertTrue(size >= min);
            assertTrue(size <= max);
            if (testInRange(size, goal.toMillis() * 3, 6) == 0) break;
            sleep(size / 3);
        }
        assertWithinRange(size, goal.toMillis() * 3, 6);
    }

    @Test
    public void testTermination() {
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6);
        Iterator<List<Integer>> iter = TimePartitioningIterator.partition(numbers, 4, 4, 100, Duration.ofSeconds(1)).iterator();
        assertEquals(iter.next(), Arrays.asList(1, 2, 3, 4));   // First batch is always minSize.
        assertEquals(iter.next(), Arrays.asList(5, 6)); // This test is fast so the rest is consumed w/the second batch.
        assertFalse(iter.hasNext());
    }

    /** Returns -1, 0, 1 if actual is less than, equal to, or greater than expected w/in epsilon. */
    private int testInRange(long actual, long expected, long epsilon) {
        long min = expected - epsilon;
        long max = expected + epsilon;
        return actual < min ? -1 : actual > max ? 1 : 0;
    }

    private void assertWithinRange(long actual, long expected, long epsilon) {
        int result = testInRange(actual, expected, epsilon);
        assertTrue(result >= 0, format("Partition too small: %d < %d", actual, expected - epsilon));
        assertTrue(result <= 0, format("Partition too large: %d > %d", actual, expected + epsilon));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
