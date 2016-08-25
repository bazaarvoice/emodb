package com.bazaarvoice.emodb.sor.core;

import com.bazaarvoice.emodb.job.api.JobHandler;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobIdentifier;
import com.bazaarvoice.emodb.job.api.JobRequest;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.job.api.JobStatus;
import com.bazaarvoice.emodb.sor.api.Audit;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.PurgeStatus;
import com.bazaarvoice.emodb.sor.api.UnknownPurgeException;
import com.google.common.base.Supplier;
import com.google.inject.Inject;

import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by andee.liao on 6/7/16.
 */
public class DefaultDataStoreAsync implements DataStoreAsync {
    private final DataStore _dataStore;
    private final JobService _jobService;

    @Inject
    public DefaultDataStoreAsync(DataStore dataStore, JobService jobService, JobHandlerRegistry jobHandlerRegistry) {
        _dataStore = dataStore;
        _jobService = jobService;

        checkNotNull(jobHandlerRegistry, "jobHandlerRegistry");
        registerPurgeJobHandler(jobHandlerRegistry);
    }

    private void registerPurgeJobHandler(JobHandlerRegistry jobHandlerRegistry) {
        jobHandlerRegistry.addHandler(
                PurgeJob.INSTANCE,
                new Supplier<JobHandler<PurgeRequest, PurgeResult>>() {
                    @Override
                    public JobHandler<PurgeRequest, PurgeResult> get() {
                        return getPurgeRequestPurgeResultJobHandler();
                    }
                });
    }

    protected JobHandler<PurgeRequest, PurgeResult> getPurgeRequestPurgeResultJobHandler() {
        return new JobHandler<PurgeRequest, PurgeResult>() {
            @Override
            public PurgeResult run(PurgeRequest request){
                _dataStore.purgeTableUnsafe(request.getTable(), request.getAudit());
                return new PurgeResult(new Date());
            }
        };
    }

    @Override
    public String purgeTableAsync(String table, Audit audit) {
        checkNotNull(audit, "audit");

        JobIdentifier<PurgeRequest, PurgeResult> jobId =
                _jobService.submitJob(
                        new JobRequest<>(PurgeJob.INSTANCE, new PurgeRequest(table, audit)));
        return jobId.toString();
    }

    @Override
    public PurgeStatus getPurgeStatus( String table, String jobID) {
        checkNotNull(table, "table");

        JobIdentifier<PurgeRequest, PurgeResult> jobId;
        try {
            jobId = JobIdentifier.fromString(jobID, PurgeJob.INSTANCE);
        } catch (IllegalArgumentException e) {
            // The tableName is illegal and therefore cannot match any purge jobs.
            throw new UnknownPurgeException(jobID);
        }

        JobStatus<PurgeRequest, PurgeResult> status = _jobService.getJobStatus(jobId);

        if (status == null) {
            throw new UnknownPurgeException(jobID);
        }

        PurgeRequest request = status.getRequest();
        if (request == null) {
            throw new IllegalStateException("Purge request details not found: " + jobId);
        }

        switch (status.getStatus()) {
            case FINISHED:
                return new PurgeStatus(request.getTable(), PurgeStatus.Status.COMPLETE);

            case FAILED:
                return new PurgeStatus(request.getTable(), PurgeStatus.Status.ERROR);

            default:
                return new PurgeStatus(request.getTable(), PurgeStatus.Status.IN_PROGRESS);
        }
    }

}
