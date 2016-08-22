package com.bazaarvoice.emodb.queue.core;

import com.bazaarvoice.emodb.job.api.JobType;

/**
 * Job type for moving a dedup queue.
 */
public class MoveDedupQueueJob extends JobType<MoveQueueRequest, MoveQueueResult> {

    final public static MoveDedupQueueJob INSTANCE = new MoveDedupQueueJob();

    private MoveDedupQueueJob() {
        super("move-dedup-queue", MoveQueueRequest.class, MoveQueueResult.class);
    }
}
