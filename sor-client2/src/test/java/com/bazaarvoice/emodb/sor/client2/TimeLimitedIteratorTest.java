package com.bazaarvoice.emodb.sor.client2;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.PeekingIterator;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TimeLimitedIteratorTest {

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullIterator() {
        TimeLimitedIterator.create(null, Duration.ZERO, 0);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullDuration() {
        TimeLimitedIterator.create(countForever(), null, 0);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativeMinimum() {
        TimeLimitedIterator.create(countForever(), Duration.ZERO, -1);
    }

    @Test
    public void testImmediateExpiration() {
        Iterator<Long> unlimitedIter = countForever();
        PeekingIterator<Long> limitedIter = TimeLimitedIterator.create(unlimitedIter, Duration.ZERO, 0);

        assertFalse(limitedIter.hasNext());

        assertEquals(unlimitedIter.next(), 0L);
    }

    @Test
    public void testMinimumOne() {
        Iterator<Long> unlimitedIter = countForever();
        PeekingIterator<Long> limitedIter = TimeLimitedIterator.create(unlimitedIter, Duration.ZERO, 1);

        assertTrue(limitedIter.hasNext());
        assertEquals(limitedIter.next(), 0L);
        assertFalse(limitedIter.hasNext());

        assertEquals(unlimitedIter.next(), 1L);
    }

    @Test
    public void testMinimumTwo() {
        Iterator<Long> unlimitedIter = countForever();
        PeekingIterator<Long> limitedIter = TimeLimitedIterator.create(unlimitedIter, Duration.ZERO, 2);

        assertTrue(limitedIter.hasNext());
        assertEquals(limitedIter.next(), 0L);
        assertTrue(limitedIter.hasNext());
        assertEquals(limitedIter.next(), 1L);
        assertFalse(limitedIter.hasNext());

        assertEquals(unlimitedIter.next(), 2L);
    }

    @Test
    public void testExpirationTime() {
        long start = System.currentTimeMillis();
        Iterator<Long> unlimitedIter = countForever();
        PeekingIterator<Long> limitedIter = TimeLimitedIterator.create(unlimitedIter, Duration.ofMillis(10), 0);
        long previous = -1;
        while (limitedIter.hasNext()) {
            long next = limitedIter.next();
            assertEquals(next, previous + 1);
            previous = next;
        }
        long stop = System.currentTimeMillis();
        long elapsed = stop - start;

        assertTrue(elapsed >= 10);
        assertTrue(elapsed < 100);  // be fairly relaxed about the upper bound to avoid spurious test failures on slow machines.
        assertEquals(unlimitedIter.next(), (previous + 1));
    }

    private void assertEquals(Long actual, long expected) {
        Assert.assertEquals(actual, (Long) expected);
    }

    private Iterator<Long> countForever() {
        return new AbstractIterator<Long>() {
            private long _counter;

            @Override
            protected Long computeNext() {
                checkState(_counter < Long.MAX_VALUE);  // don't rollover
                return _counter++;
            }
        };
    }
}
