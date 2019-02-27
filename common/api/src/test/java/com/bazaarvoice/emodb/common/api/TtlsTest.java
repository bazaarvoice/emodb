package com.bazaarvoice.emodb.common.api;

import org.testng.annotations.Test;

import java.time.Duration;

import static org.testng.Assert.assertEquals;

public class TtlsTest {

    @Test
    public void testZero() {
        assertEquals(Ttls.toSeconds(Duration.ZERO, 0, null), (Integer) 0);
        assertEquals(Ttls.toSeconds(Duration.ZERO, 1, 10), (Integer) 1);
    }

    @Test
    public void testSmall() {
        assertEquals(Ttls.toSeconds(Duration.ofMillis(1), 0, null), (Integer) 1);
        assertEquals(Ttls.toSeconds(Duration.ofMillis(1), 2, null), (Integer) 2);
    }

    @Test
    public void testOneYearLimit() {
        int oneYear = Math.toIntExact(Duration.ofDays(365).getSeconds());
        assertEquals(Ttls.toSeconds(Duration.ofMillis(1), 1, oneYear), (Integer) 1);
        assertEquals(Ttls.toSeconds(Duration.ofDays(366), 1, oneYear), (Integer) oneYear);
    }

    @Test
    public void testLarge() {
        assertEquals(Ttls.toSeconds(Duration.ofSeconds(Integer.MAX_VALUE), 0, null), (Integer) Integer.MAX_VALUE);
        assertEquals(Ttls.toSeconds(Duration.ofSeconds(Integer.MAX_VALUE + 1L), 0, null), null);

        assertEquals(Ttls.toSeconds(Duration.ofSeconds(Integer.MAX_VALUE), 0, Integer.MAX_VALUE), (Integer) Integer.MAX_VALUE);
        assertEquals(Ttls.toSeconds(Duration.ofSeconds(Integer.MAX_VALUE + 1L), 0, Integer.MAX_VALUE), (Integer) Integer.MAX_VALUE);
    }

    @Test
    public void testForever() {
        assertEquals(Ttls.toSeconds(null, 0, null), null);
        assertEquals(Ttls.toSeconds(null, 0, Integer.MAX_VALUE), (Integer) Integer.MAX_VALUE);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegative() {
        Ttls.toSeconds(Duration.ofSeconds(-1), 0, null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNegativeLong() {
        Ttls.toSeconds(Duration.ofMillis(Long.MIN_VALUE), 0, null);
    }
}
