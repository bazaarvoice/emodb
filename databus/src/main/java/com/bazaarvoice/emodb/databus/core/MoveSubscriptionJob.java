package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.job.api.JobType;

/**
 * Job type for moving a subscription.
 */
public class MoveSubscriptionJob extends JobType<MoveSubscriptionRequest, MoveSubscriptionResult> {

    final public static MoveSubscriptionJob INSTANCE = new MoveSubscriptionJob();

    private MoveSubscriptionJob() {
        super("move-subscription", MoveSubscriptionRequest.class, MoveSubscriptionResult.class);
    }
}
