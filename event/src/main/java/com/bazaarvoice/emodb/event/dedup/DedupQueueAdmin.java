package com.bazaarvoice.emodb.event.dedup;

import java.util.Map;

public interface DedupQueueAdmin {
    /**
     * Return a snapshot of currently active dedup queues.
     */
    Map<String, DedupQueue> getActiveQueues();

    /**
     * Start the leadership election and queue background filling process for a dedup queue w/o causing other side
     * effects.  Useful for testing and debugging.
     * @return true if activation was successful, false if it timed out or if it failed because the current server
     * is not the owner of the queue.
     */
    boolean activateQueue(String queue);
}
