package com.bazaarvoice.emodb.job.api;

import com.google.common.base.Supplier;

public interface JobHandlerRegistry {

    /**
     * Adds or replaces the handler for a specific job type.  Each job will run from a JobHandler as returned by the
     * supplier.
     * <p>
     * Because there is state associated with the handler the Supplier must return a new instance on each call to
     * {@link com.google.common.base.Supplier#get()}.
     * @param jobType The job type
     * @param handlerSupplier Supplier of handlers for this job type
     * @param <Q> The class for making requests
     * @param <R> The class for job results
     * @return True if this handler replaces an existing handler, false if there was no existing handler
     */
    <Q, R> boolean addHandler(JobType<Q, R> jobType, Supplier<JobHandler<Q, R>> handlerSupplier);
}
