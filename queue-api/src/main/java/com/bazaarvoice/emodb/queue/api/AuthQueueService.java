package com.bazaarvoice.emodb.queue.api;

import com.bazaarvoice.emodb.auth.proxy.Credential;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Authenticating interface to {@link QueueService}.  This method should exactly mirror QueueService except with added
 * credentials in each call.
 * @see QueueService
 */
public interface AuthQueueService {
    void send(@Credential String apiKey, String queue, Object message);

    void sendAll(@Credential String apiKey, String queue, Collection<?> messages);

    void sendAll(@Credential String apiKey, Map<String, ? extends Collection<?>> messagesByQueue);

    /**
     * Counts pending messages for the specified queue.  The count will include messages that are currently claimed
     * and not returned by the {@link #poll} method.
     * <p/>
     * Note this operation is <em>expensive</em> and primarily useful for debugging.  For regular use, call
     * {@link #getMessageCountUpTo(String, String, long)} with a reasonable limit.
     */
    long getMessageCount(@Credential String apiKey, String queue);

    /**
     * Counts the total number of messages for the specified queue, accurate up to the specified limit.  Beyond the
     * specified limit the message count will be a rough estimate, allowing the caller to make the trade-off between
     * accuracy and performance.  A good choice of {@code limit} is 500--it allows distinguishing between "a few" and
     * "lots" and is reasonably performant.
     */
    long getMessageCountUpTo(@Credential String apiKey, String queue, long limit);

    /** Counts the number of messages with outstanding claims that cause the messages to be skipped by {@link #poll}. */
    long getClaimCount(@Credential String apiKey, String queue);

    List<Message> peek(@Credential String apiKey, String queue, int limit);

    List<Message> poll(@Credential String apiKey, String queue, Duration claimTtl, int limit);

    void renew(@Credential String apiKey, String queue, Collection<String> messageIds, Duration claimTtl);

    void acknowledge(@Credential String apiKey, String queue, Collection<String> messageIds);

    /**
     * Moves outstanding messages from one queue to another.  Future messages are not affected.  No guarantees are
     * made regarding message TTLs--a message about to expire may or may not have its TTL reset.
     * This method returns immediately with a reference that can be used to query the progress of the move.
     */
    String moveAsync(@Credential String apiKey, String from, String to);

    /**
     * Checks the status of a move operation.  If the reference is unknown or the move failed then this method will throw an exception.
     */
    MoveQueueStatus getMoveStatus(@Credential String apiKey, String reference);

    /** Release all outstanding claims, for debugging/testing. */
    void unclaimAll(@Credential String apiKey, String queue);

    /** Delete all messages in the queue, for debugging/testing. */
    void purge(@Credential String apiKey, String queue);
}
