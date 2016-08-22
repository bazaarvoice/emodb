package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.job.api.JobType;

/**
 * Job type for replaying the last two days for a subscription.
 */
public class ReplaySubscriptionJob extends JobType<ReplaySubscriptionRequest, ReplaySubscriptionResult> {
    public final static ReplaySubscriptionJob INSTANCE = new ReplaySubscriptionJob();

    private ReplaySubscriptionJob() {
        super("replay-subscription", ReplaySubscriptionRequest.class, ReplaySubscriptionResult.class);
    }
}
