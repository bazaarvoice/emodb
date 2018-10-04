package com.bazaarvoice.emodb.databus.api;

import com.bazaarvoice.emodb.auth.proxy.Credential;
import com.bazaarvoice.emodb.sor.condition.Condition;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

/**
 * Authenticating interface to {@link Databus}.  This method should exactly mirror Databus except with added credentials
 * in each call.
 * @see Databus
 */
public interface AuthDatabus {
    Iterator<Subscription> listSubscriptions(@Credential String apiKey, @Nullable String fromSubscriptionExclusive, long limit);

    void subscribe(@Credential String apiKey, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl);

    void subscribe(@Credential String apiKey, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl, int numKafkaTopicPartitions,
                   int kafkaTopicReplicationFactor, String kafkaTopicCleanupPolicy, String kafkaTopicCompressionType, long kafkaTopicDeleteRetentionMs, int kafkaTopicMaxMessageBytes,
                   double kafkaTopicMinCleanableDirtyRatio, int kafkaTopicMinInSyncReplicas, long kafkaTopicRetentionMs);

    void subscribe(@Credential String apiKey, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl, boolean includeDefaultJoinFilter);

    void unsubscribe(@Credential String apiKey, String subscription);

    /** Returns information about the specified subscription. */
    Subscription getSubscription(@Credential String apiKey, String subscription)
            throws UnknownSubscriptionException;

    /**
     * Counts pending events for the specified subscription.  The count will include events that are currently claimed
     * and not returned by the {@link #poll} method.
     * <p/>
     * Note this operation is <em>expensive</em> and primarily useful for debugging.  For regular use, call
     * {@link #getEventCountUpTo(String, String, long)} with a reasonable limit.
     */
    long getEventCount(@Credential String apiKey, String subscription);

    /**
     * Counts events for the specified subscription, accurate up to the specified limit.  Beyond the specified limit
     * the event count will be a rough estimate, allowing the caller to make the trade-off between accuracy and
     * performance.  A good choice of {@code limit} is 500--it allows distinguishing between "a few" and "lots" and
     * is reasonably performant.
     */
    long getEventCountUpTo(@Credential String apiKey, String subscription, long limit);

    /** Counts the number of events with outstanding claims that cause the events to be skipped by {@link #poll}. */
    long getClaimCount(@Credential String apiKey, String subscription);

    /**
     * Returns the next {@code limit} events for a subscription without claiming the events.
     * <p/>
     * Note that there is no API for paging through all events.  The {@code limit} argument is limited by the amount
     * of memory required to hold the event data on the server and, in practice, should be no more than a few hundred.
     */
    Iterator<Event> peek(@Credential String apiKey, String subscription, int limit);

    /**
     * Claim events for the specified subscription and return it.  The caller must call {@link #acknowledge} with the
     * event keys or else the events may be returned by a future call to <code>poll</code>.
     * <p/>
     * The {@code limit} argument is limited by the amount of memory required to hold the event data on the server
     * and, in practice, should be no more than a few hundred.
     * <p/>
     * This method makes <em>no</em> guarantees about whether events will be returned
     * in the order they occurred and it makes <em>no</em> guarantees about whether
     * an event will be returned multiple times.  In practice, it will try to return
     * events in order without duplicates, but there are no promises.
     */
    PollResult poll(@Credential String apiKey, String subscription, Duration claimTtl, int limit);

    /** Renew the claims on events previously returned by {@link #poll}. */
    void renew(@Credential String apiKey, String subscription, Collection<String> eventKeys, Duration claimTtl);

    /**
     * Acknowledge that events previously returned by {@link #poll} have been processed
     * successfully and <tt>poll</tt> should not return them again.
     */
    void acknowledge(@Credential String apiKey, String subscription, Collection<String> eventKeys);

    /**
     * Replays events from the last two days for the given subscription.  This method returns immediately with
     * a reference that can be used to query the progress of the replay.
     */
    String replayAsync(@Credential String apiKey, String subscription);

    /**
     * Replays events since a given timestamp within the last two days for the given subscription.
     * This method returns immediately with a reference that can be used to query the progress of the replay.
     * NOTE: This may replay some extra events that are before the 'since' timestamp (no more than 999 previous events),
     + but guarantees that any events on or after 'since' will be replayed.
     * @param since Specifies timestamp since when the events will be replayed (inclusive)
     */
    String replayAsyncSince(@Credential String apiKey, String subscription, Date since);

    /**
     * Checks the status of a replay operation.  If the reference is unknown or the replay failed then this method will
     * throw an exception.
     */
    ReplaySubscriptionStatus getReplayStatus(@Credential String apiKey, String reference);

    /**
     * Moves events from one subscription to another.  This moves all currently un-acked events and does not filter
     * by the destination subscription table filter.  Future events are not affected.  No guarantees are made
     * regarding event TTLs--an event about to expire may or may not have its TTL reset.
     * This method returns immediately with a reference that can be used to query the progress of the move.
     */
    String moveAsync(@Credential String apiKey, String from, String to);

    /**
     * Checks the status of a move operation.  If the reference is unknown or the move failed then this method will throw an exception.
     */
    MoveSubscriptionStatus getMoveStatus(@Credential String apiKey, String reference);

    /** Create an artificial event for a subscription, for debugging/testing. */
    void injectEvent(@Credential String apiKey, String subscription, String table, String key);

    /** Release all outstanding claims, for debugging/testing. */
    void unclaimAll(@Credential String apiKey, String subscription);

    /** Delete all events pending for the subscription, for debugging/testing. */
    void purge(@Credential String apiKey, String subscription);

}
