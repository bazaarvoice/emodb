package com.bazaarvoice.emodb.job.dao;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.job.api.JobIdentifier;
import com.bazaarvoice.emodb.job.api.JobStatus;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.Map;

import static com.bazaarvoice.emodb.job.util.JobStatusUtil.narrow;

public class InMemoryJobStatusDAO implements JobStatusDAO {

    private final Map<JobIdentifier<?, ?>, Object> _statusMap = Maps.newConcurrentMap();

    @Override
    public <Q, R> void updateJobStatus(JobIdentifier<Q, R> jobId, JobStatus<Q, R> jobStatus) {
        _statusMap.put(jobId, JsonHelper.convert(jobStatus, Map.class));
    }

    @Nullable
    @Override
    public <Q, R> JobStatus<Q, R> getJobStatus(final JobIdentifier<Q, R> jobId) {
        Object jobStatusJson = _statusMap.get(jobId);
        if (jobStatusJson == null) {
            return null;
        }

        return narrow(jobStatusJson, jobId.getJobType());
    }

    @Override
    public void deleteJobStatus(JobIdentifier<?, ?> jobId) {
        _statusMap.remove(jobId);
    }
}
