package com.bazaarvoice.emodb.job.handler;

import com.bazaarvoice.emodb.job.api.JobHandler;
import com.bazaarvoice.emodb.job.api.JobType;
import com.google.common.base.Supplier;

import static com.google.common.base.Preconditions.checkNotNull;

public class RegistryEntry<Q, R> {
    private final JobType<Q, R> _jobType;
    private final Supplier<JobHandler<Q, R>> _handlerSupplier;

    public RegistryEntry(JobType<Q, R> jobType, Supplier<JobHandler<Q, R>> handlerSupplier) {
        _jobType = checkNotNull(jobType, "jobType");
        _handlerSupplier = checkNotNull(handlerSupplier, "handlerSupplier");
    }

    public JobType<Q, R> getJobType() {
        return _jobType;
    }

    public JobHandler<Q, R> newHandler() {
        return _handlerSupplier.get();
    }
}
