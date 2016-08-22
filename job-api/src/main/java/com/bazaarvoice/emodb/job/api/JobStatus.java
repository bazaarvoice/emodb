package com.bazaarvoice.emodb.job.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class JobStatus<Q, R> {

    public enum Status {
        // The job has been submitted but is not yet executing
        SUBMITTED,
        // The job is currently being run
        RUNNING,
        // The job finished without error
        FINISHED,
        // The job failed
        FAILED
    }

    private final Status _status;
    private final Q _request;
    private final R _result;
    private final String _errorMessage;

    @JsonCreator
    public JobStatus(
            @JsonProperty ("status") Status status,
            @JsonProperty ("request") @Nullable Q request,
            @JsonProperty ("result") @Nullable R result,
            @JsonProperty ("errorMessage") @Nullable String errorMessage) {
        _status = checkNotNull(status, "status");
        _request = request;
        _result = result;
        _errorMessage = errorMessage;
    }

    public Status getStatus() {
        return _status;
    }

    @Nullable
    public Q getRequest() {
        return _request;
    }

    @Nullable
    public R getResult() {
        return _result;
    }

    @Nullable
    public String getErrorMessage() {
        return _errorMessage;
    }
}
