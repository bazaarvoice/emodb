package com.bazaarvoice.emodb.job.handler;

import com.bazaarvoice.emodb.job.api.JobHandler;
import com.bazaarvoice.emodb.job.api.JobType;
import com.google.common.base.Supplier;
import com.google.common.collect.Maps;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class DefaultJobHandlerRegistry implements JobHandlerRegistryInternal {

    private Map<String, RegistryEntry<?, ?>> _entries = Maps.newConcurrentMap();

    @Override
    public <Q, R> boolean addHandler(JobType<Q, R> jobType, Supplier<JobHandler<Q, R>> handlerSupplier) {
        requireNonNull(jobType, "jobType");
        requireNonNull(handlerSupplier, "handlerSupplier");
        RegistryEntry<?, ?> existing = _entries.put(jobType.getName(), new RegistryEntry<>(jobType, handlerSupplier));
        return existing != null;
    }

    @Override
    public RegistryEntry<?, ?> getRegistryEntry(String jobName) {
        requireNonNull(jobName, "jobName");
        return _entries.get(jobName);
    }
}
