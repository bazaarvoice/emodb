package com.bazaarvoice.emodb.queue.api;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface QueueService extends BaseQueueService {
    // Note: all the methods in BaseQueueService are copied here w/o modification to work around
    // a bug in Ostrich 1.7.1 and older that breaks the Ostrich client consistent hashing.

    void send(String queue, Object message);

    void sendAll(String queue, Collection<?> messages);


    void sendAll(Map<String, ? extends Collection<?>> messagesByQueue);

    //Overloaded sendAll method to send to cassandra
    void sendAll(String queue, Collection<?> messages, boolean isFlush);

    /**
     * Counts pending messages for the specified queue.  The count will include messages that are currently claimed
     * and not returned by the {@link #poll} method.
     * <p/>
     * Note this operation is <em>expensive</em> and primarily useful for debugging.  For regular use, call
     * {@link #getMessageCountUpTo(String, long)} with a reasonable limit.
     */
    long getMessageCount(String queue);

    /**
     * Counts the total number of messages for the specified queue, accurate up to the specified limit.  Beyond the
     * specified limit the message count will be a rough estimate, allowing the caller to make the trade-off between
     * accuracy and performance.  A good choice of {@code limit} is 500--it allows distinguishing between "a few" and
     * "lots" and is reasonably performant.
     */
    long getMessageCountUpTo(String queue, long limit);

    /** Counts the number of messages with outstanding claims that cause the messages to be skipped by {@link #poll}. */
    long getClaimCount(String queue);

    List<Message> peek(String queue, int limit);

    List<Message> poll(String queue, Duration claimTtl, int limit);

    void renew(String queue, Collection<String> messageIds, Duration claimTtl);

    void acknowledge(String queue, Collection<String> messageIds);

    /**
     * Moves outstanding messages from one queue to another.  Future messages are not affected.  No guarantees are
     * made regarding message TTLs--a message about to expire may or may not have its TTL reset.
     * This method returns immediately with a reference that can be used to query the progress of the move.
     */
    String moveAsync(String from, String to);

    /**
     * Checks the status of a move operation.  If the reference is unknown or the move failed then this method will throw an exception.
     */
    MoveQueueStatus getMoveStatus(String reference);

    /** Release all outstanding claims, for debugging/testing. */
    void unclaimAll(String queue);

    /** Delete all messages in the queue, for debugging/testing. */
    void purge(String queue);
}
