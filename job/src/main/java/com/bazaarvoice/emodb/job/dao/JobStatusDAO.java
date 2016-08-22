package com.bazaarvoice.emodb.job.dao;

import com.bazaarvoice.emodb.job.api.JobIdentifier;
import com.bazaarvoice.emodb.job.api.JobStatus;

import javax.annotation.Nullable;

public interface JobStatusDAO {

    <Q, R> void updateJobStatus(JobIdentifier<Q, R> jobId, JobStatus<Q, R> jobStatus);

    @Nullable
    <Q, R> JobStatus<Q, R> getJobStatus(JobIdentifier<Q, R> jobId);

    void deleteJobStatus(JobIdentifier<?, ?> jobId);
}
