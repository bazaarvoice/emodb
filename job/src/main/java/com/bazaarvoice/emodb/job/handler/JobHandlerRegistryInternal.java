package com.bazaarvoice.emodb.job.handler;

import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;

public interface JobHandlerRegistryInternal extends JobHandlerRegistry {

    RegistryEntry<?, ?> getRegistryEntry(String jobName);
}
