package com.bazaarvoice.emodb.event.core;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.joda.time.Duration;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class DefaultClaimSetTest {
    private static final Duration TTL = Duration.standardHours(1);

    @Test
    public void testClaim() {
        ClaimSet claimSet = new DefaultClaimSet();
        byte[] claim = newClaim(1);
        assertEquals(claimSet.size(), 0);

        // Stake a claim and verify that it is claimed.
        assertFalse(claimSet.isClaimed(claim));
        assertTrue(claimSet.acquire(claim, TTL));
        assertTrue(claimSet.isClaimed(claim));
        assertFalse(claimSet.isClaimed(newClaim(2)));
        assertEquals(claimSet.size(), 1);

        // Try to stake a second claim.  It should fail, but the event should still be claimed.
        assertFalse(claimSet.acquire(claim, TTL));
        assertTrue(claimSet.isClaimed(claim));

        // Renew the claim but don't extend it, verify the original TTL still holds.
        claimSet.renew(claim, Duration.ZERO, true);
        assertTrue(claimSet.isClaimed(claim));

        // The API doesn't support delete because the EventStore doesn't use it.  But renewing for 0 seconds with
        // extendOnly==false and pumping events has the same effect.
        claimSet.renew(claim, Duration.ZERO, false);
        assertFalse(claimSet.isClaimed(claim));
        assertEquals(claimSet.size(), 0);

        // Claim it again, it should succeed
        assertTrue(claimSet.acquire(claim, TTL));
        assertTrue(claimSet.isClaimed(claim));
        assertEquals(claimSet.size(), 1);

        // Pump makes some assertions that the ClaimSet data structures are in sync
        claimSet.pump();
    }

    @Test
    public void testRenewExpiredClaim() {
        ClaimSet claimSet = new DefaultClaimSet();
        byte[] claim = newClaim(1);

        assertTrue(claimSet.acquire(claim, Duration.ZERO));
        assertFalse(claimSet.isClaimed(claim));

        claimSet.renew(claim, TTL, false);
        assertTrue(claimSet.isClaimed(claim));
    }

    @Test
    public void testMultipleClaims() throws Exception {
        ClaimSet claimSet = new DefaultClaimSet();
        int ttlGranularityMillis = 50;  // You may want to increase this to 10000 when debugging.
        Random random = new Random();

        // Create a handful of claims with varying TTLs
        Map<ByteBuffer, Long> claimToTtl = Maps.newHashMap();
        Set<Long> distinctTtls = Sets.newTreeSet();
        for (int i = 0; i < 50; i++) {
            byte[] claim = newClaim(i);
            long ttlMillis = ttlGranularityMillis * (1 + random.nextInt(5));
            assertTrue(claimSet.acquire(claim, Duration.millis(ttlMillis)));

            assertTrue(claimSet.isClaimed(claim));
            assertEquals(claimSet.size(), i + 1);

            claimToTtl.put(ByteBuffer.wrap(claim), ttlMillis);
            distinctTtls.add(ttlMillis);
        }
        long start = System.currentTimeMillis();

        // For each distinct TTL, sleep until we're pretty sure all the claims at that TTL have expired.  Verify
        // we can re-acquire those claims, but other claims are still taken.  This is timing-related.  If it runs too
        // slow it can fail, but the ttlGranularityMillis was chosen to make the chance of failure very small.
        for (long ttlMillis : distinctTtls) {
            sleepUntil(start + ttlMillis);

            for (Map.Entry<ByteBuffer, Long> entry : claimToTtl.entrySet()) {
                byte[] claim = entry.getKey().array();
                long actualTtlMillis = entry.getValue();
                assertEquals(claimSet.acquire(claim, TTL), actualTtlMillis == ttlMillis);
            }
            assertEquals(claimSet.size(), claimToTtl.size());
        }

        // Pump makes some assertions that the ClaimSet data structures are in sync
        claimSet.pump();
    }

    /**
     * Sanity check that the basic ClaimSet operations are O(1) or O(log N), not O(n).
     * <p>
     * This test requires at 64MB of free memory.  If it fails because it's too slow, check your -Xmx settings first
     * and make sure they're sufficient.  In testing, performance with 48MB of memory was 10x slower than performance
     * with 64MB of memory.
     */
    @Test
    public void testManyTtlPerformance() {
        long start = System.currentTimeMillis();

        ClaimSet claimSet = new DefaultClaimSet();
        int reps = 100000;  // This has to be big enough to make O(n) behavior apparent, if any
        for (int i = 0; i < reps; i++) {
            assertTrue(claimSet.acquire(newClaim(i), Duration.millis(i)));
        }
        for (int i = 0; i < reps; i++) {
            claimSet.renew(newClaim(i), Duration.millis(i + 1), true);
        }
        for (int i = 0; i < reps; i++) {
            claimSet.renew(newClaim(i), Duration.ZERO, false);
        }
        for (int i = 0; i < reps; i++) {
            claimSet.acquire(newClaim(i), Duration.millis(i));
        }
        claimSet.pump();

        // In testing, a bad implementation of ClaimSet took 681 seconds.  The current implementation takes <1 second.
        long stop = System.currentTimeMillis();
        long elapsedSeconds = TimeUnit.MILLISECONDS.toSeconds(stop - start);
        assertTrue(elapsedSeconds < 30, "ClaimSet performance is significantly worse than expected: " + elapsedSeconds);
    }

    private void sleepUntil(long time) throws InterruptedException {
        for (;;) {
            long sleep = time - System.currentTimeMillis();
            if (sleep <= 0) {
                break;
            }
            Thread.sleep(sleep);
        }
    }

    private byte[] newClaim(int i) {
        return ByteBuffer.allocate(4).putInt(i).array();
    }
}
