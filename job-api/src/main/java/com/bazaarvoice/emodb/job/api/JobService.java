package com.bazaarvoice.emodb.job.api;

/**
 * Interface for submitting jobs and retrieving job status.
 */
public interface JobService {

    /**
     * Submits a new job to the JobService.
     * @param jobRequest The request that encapsulates the job type and parameters.
     * @param <Q> The job request type
     * @param <R> The job response type
     * @return An identifier that can be used to query for the job's status
     */
    <Q, R> JobIdentifier<Q, R> submitJob(JobRequest<Q, R> jobRequest);

    /**
     * Queries for the status of a job created by {@link #submitJob(JobRequest)}
     * @param id  The identifier returned from the original submit request.
     * @param <Q> The job request type
     * @param <R> The job response type
     * @return The job's status, or null if no such job exists
     */
    <Q, R> JobStatus<Q, R> getJobStatus(JobIdentifier<Q, R> id);

    /**
     * Stops the job service from processing any new jobs from the queue.  Note that this does not affect any jobs
     * already in progress at the time of the call.
     * @return true if the service had been running and is now paused, false if this service was already paused
     */
    boolean pause();

    /**
     * Resumes the job service after a prior call to {@link #pause()}.
     * @return true if the service had been paused and is now running, false if this server was already running
     */
    boolean resume();
}
