package com.bazaarvoice.emodb.job.handler;

import com.bazaarvoice.emodb.job.api.JobHandler;
import com.bazaarvoice.emodb.job.api.JobType;
import com.google.common.base.Supplier;

import static java.util.Objects.requireNonNull;

public class RegistryEntry<Q, R> {
    private final JobType<Q, R> _jobType;
    private final Supplier<JobHandler<Q, R>> _handlerSupplier;

    public RegistryEntry(JobType<Q, R> jobType, Supplier<JobHandler<Q, R>> handlerSupplier) {
        _jobType = requireNonNull(jobType, "jobType");
        _handlerSupplier = requireNonNull(handlerSupplier, "handlerSupplier");
    }

    public JobType<Q, R> getJobType() {
        return _jobType;
    }

    public JobHandler<Q, R> newHandler() {
        return _handlerSupplier.get();
    }
}
