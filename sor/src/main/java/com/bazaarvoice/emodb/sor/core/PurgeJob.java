package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.job.api.JobType;

public class PurgeJob extends JobType<PurgeRequest, PurgeResult> {

    final public static PurgeJob INSTANCE = new PurgeJob();

    private PurgeJob() {
        super("purge", PurgeRequest.class, PurgeResult.class);
    }
}
