package com.bazaarvoice.emodb.event.core;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.PriorityQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/** In-memory implementation of the {@link ClaimSet} interface. */
public class DefaultClaimSet implements ClaimSet {
    /** Holds claim objects.  Supports O(1) lookup by Claim ID. */
    private final Map<Claim, Claim> _claimMap = Maps.newHashMap();
    /** Map of TtlMillis to a queue of claims in the order they expire. */
    private final Map<Long, LinkedHashSet<Claim>> _expirationQueues = Maps.newHashMap();
    /** Sorts expiration queues by the expiration of their head element at at the time of priority queue insertion. */
    private final PriorityQueue<ProcessAt> _schedule = new PriorityQueue<>();

    @Override
    public synchronized long size() {
        processExpirationQueues(System.currentTimeMillis());
        return _claimMap.size();
    }

    @Override
    public synchronized boolean isClaimed(byte[] claimId) {
        checkNotNull(claimId, "claimId");

        long now = System.currentTimeMillis();
        processExpirationQueues(now);

        return _claimMap.containsKey(new Claim(claimId, 0, 0));
    }

    @Override
    public synchronized boolean acquire(byte[] claimId, Duration ttl) {
        checkNotNull(claimId, "claimId");
        long ttlMillis = ttl.toMillis();
        checkArgument(ttlMillis >= 0, "Ttl must be >=0");

        long now = System.currentTimeMillis();
        processExpirationQueues(now);

        Claim claim = new Claim(claimId, ttlMillis, now + ttlMillis);
        if (_claimMap.containsKey(claim)) {
            return false;
        }

        _claimMap.put(claim, claim);
        addToExpirationQueue(claim);
        return true;
    }

    @Override
    public void renew(byte[] claimId, Duration ttl, boolean extendOnly) {
        renewAll(Collections.singleton(claimId), ttl, extendOnly);
    }

    @Override
    public synchronized void renewAll(Collection<byte[]> claimIds, Duration ttl, boolean extendOnly) {
        checkNotNull(claimIds, "claimIds");
        long ttlMillis = ttl.toMillis();
        checkArgument(ttlMillis >= 0, "Ttl must be >=0");

        long now = System.currentTimeMillis();
        processExpirationQueues(now);

        for (byte[] claimId : claimIds) {
            Claim claim = new Claim(claimId, ttlMillis, now + ttlMillis);
            Claim oldClaim = _claimMap.put(claim, claim);

            if (extendOnly && oldClaim != null && oldClaim.getExpireAt() >= claim.getExpireAt()) {
                // Old claim is for longer than the new claim and 'extendOnly' means don't shorten the life of a claim
                _claimMap.put(oldClaim, oldClaim);
            } else {
                if (oldClaim != null) {
                    removeFromExpirationQueue(oldClaim);
                }
                addToExpirationQueue(claim);
            }
        }
    }

    @Override
    public synchronized void clear() {
        _claimMap.clear();
        _expirationQueues.clear();
        _schedule.clear();
    }

        @Override
    public synchronized void pump() {
        processExpirationQueues(System.currentTimeMillis());
        removeEmptyExpirationQueues();
    }

    private void addToExpirationQueue(Claim claim) {
        Long ttlMillis = claim.getTtlMillis();
        LinkedHashSet<Claim> queue = _expirationQueues.get(ttlMillis);
        if (queue == null) {
            queue = Sets.newLinkedHashSet();
            queue.add(claim);
            _expirationQueues.put(ttlMillis, queue);
            _schedule.offer(new ProcessAt(queue, ttlMillis));
        } else {
            queue.add(claim);
        }
    }

    private void removeFromExpirationQueue(Claim claim) {
        LinkedHashSet<Claim> queue = _expirationQueues.get(claim.getTtlMillis());
        if (queue != null) {
            queue.remove(claim);
        }
        // Don't remove queue from _schedule if it's empty since removing from a PriorityQueue is O(n)
    }

    private void removeEmptyExpirationQueues() {
        // Periodically go through and remove empty expiration queues so they don't accumulate forever.
        // This cleans up queues with really long TTLs where every member was explicitly removed.
        Iterables.removeIf(_expirationQueues.values(), new Predicate<LinkedHashSet<Claim>>() {
            @Override
            public boolean apply(LinkedHashSet<Claim> queue) {
                return queue.isEmpty();
            }
        });
        Iterables.removeIf(_schedule, new Predicate<ProcessAt>() {
            @Override
            public boolean apply(ProcessAt processAt) {
                return processAt.getQueue().isEmpty();
            }
        });
        checkState(_schedule.size() == _expirationQueues.size());
    }

    private void processExpirationQueues(long now) {
        ProcessAt processAt;
        while ((processAt = _schedule.peek()) != null) {
            // If the entry at the head of the priority queue isn't due, we're done.
            if (processAt.getTimestamp() > now) {
                break;
            }
            ProcessAt removed = _schedule.remove();
            checkState(removed == processAt);

            // Remove all the expired claims in the claim queue.
            LinkedHashSet<Claim> queue = processAt.getQueue();
            long ttlMillis = processAt.getTtlMillis();
            processExpirationQueue(now, queue);

            // Schedule the claim queue for processing next time around based on its new head element.
            if (!queue.isEmpty()) {
                _schedule.offer(new ProcessAt(queue, ttlMillis));
            } else {
                _expirationQueues.remove(ttlMillis);
            }
        }
        checkState(_schedule.size() == _expirationQueues.size());
    }

    private void processExpirationQueue(long now, LinkedHashSet<Claim> queue) {
        Iterator<Claim> claimIter = queue.iterator();
        while (claimIter.hasNext()) {
            Claim claim = claimIter.next();
            // If the claim at the head of the queue hasn't expired yet, we're done processing this queue.
            if (claim.getExpireAt() > now) {
                break;
            }
            _claimMap.remove(claim);
            claimIter.remove();
        }
    }

    /**
     * Wraps a single claim.  Tracks the claim's TTL and expiration time and implements equals and hashCode on the
     * ID so it can be used as the key in the claim map.
     */
    private static class Claim {
        private final byte[] _id;
        private final long _ttlMillis;
        private final long _expireAt;

        private Claim(byte[] id, long ttlMillis, long expireAt) {
            _id = id;
            _ttlMillis = ttlMillis;
            _expireAt = expireAt;
        }

        long getTtlMillis() {
            return _ttlMillis;
        }

        long getExpireAt() {
            return _expireAt;
        }

        @Override
        public boolean equals(Object o) {
            // Ignore ttlMillis and expireAt so we can find claims by ID in a hash table.
            return this == o || (o instanceof Claim && Arrays.equals(_id, ((Claim) o)._id));
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(_id);
        }
    }

    /**
     *
     */
    private static class ProcessAt implements Comparable<ProcessAt> {
        private final long _timestamp;
        private final LinkedHashSet<Claim> _queue;
        private final long _ttlMillis;

        private ProcessAt(LinkedHashSet<Claim> queue, long ttlMillis) {
            _timestamp = queue.iterator().next().getExpireAt();
            _queue = queue;
            _ttlMillis = ttlMillis;
        }

        long getTimestamp() {
            return _timestamp;
        }

        LinkedHashSet<Claim> getQueue() {
            return _queue;
        }

        long getTtlMillis() {
            return _ttlMillis;
        }

        @Override
        public int compareTo(ProcessAt processAt) {
            return Longs.compare(_timestamp, processAt.getTimestamp());
        }
    }
}
