package com.bazaarvoice.emodb.queue.core;

import com.bazaarvoice.emodb.job.api.JobType;

/**
 * Job type for moving a queue.
 */
public class MoveQueueJob extends JobType<MoveQueueRequest, MoveQueueResult> {

    final public static MoveQueueJob INSTANCE = new MoveQueueJob();

    private MoveQueueJob() {
        super("move-queue", MoveQueueRequest.class, MoveQueueResult.class);
    }
}
