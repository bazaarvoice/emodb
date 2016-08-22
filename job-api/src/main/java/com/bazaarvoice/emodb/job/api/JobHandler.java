package com.bazaarvoice.emodb.job.api;

/**
 * Superclass for classes which define how jobs should be executed.  Subclasses should only have to override a single
 * method, {@link #run(Object)}.  This method should return the result of running the job for the given request.
 * If the job cannot be run locally because it does not own a resource required for execution it can return
 * {@link #notOwner()}.
 * @param <Q> The type for job requests
 * @param <R> The type for job responses
 */
abstract public class JobHandler<Q, R> {

    private boolean _notOwner = false;

    abstract public R run(Q request) throws Exception;

    protected final R notOwner() {
        _notOwner = true;
        return null;
    }

    boolean isNotOwner() {
        return _notOwner;
    }
}
