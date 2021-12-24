package com.bazaarvoice.emodb.job;

import java.time.Duration;

import static java.util.Objects.requireNonNull;

public class JobConfiguration {

    private final static int DEFAULT_CONCURRENCY_LEVEL = 2;
    private final static Duration DEFAULT_QUEUE_REFRESH_TIME = Duration.ofSeconds(10);
    private final static int DEFAULT_QUEUE_PEEK_LIMIT = 100;
    private final static Duration DEFAULT_NOT_OWNER_RETRY_DELAY = Duration.ofMinutes(5);

    // Number of jobs that can be run concurrently (job thread pool size)
    private int _concurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;
    // How long the head of the queue should be cached locally before refreshing
    private Duration _queueRefreshTime = DEFAULT_QUEUE_REFRESH_TIME;
    // Maximum number of entries to peek from the queue on each refresh
    private int _queuePeekLimit = DEFAULT_QUEUE_PEEK_LIMIT;
    // Minimum amount of time to wait before retrying a job locally which had returned "notOwner" on the last run
    private Duration _notOwnerRetryDelay = DEFAULT_NOT_OWNER_RETRY_DELAY;

    public int getConcurrencyLevel() {
        return _concurrencyLevel;
    }

    public JobConfiguration setConcurrencyLevel(int concurrencyLevel) {
        _concurrencyLevel = concurrencyLevel;
        return this;
    }

    public Duration getQueueRefreshTime() {
        return _queueRefreshTime;
    }

    public void setQueueRefreshTime(Duration queueRefreshTime) {
        _queueRefreshTime = requireNonNull(queueRefreshTime, "queueRefreshTime");
    }

    public int getQueuePeekLimit() {
        return _queuePeekLimit;
    }

    public void setQueuePeekLimit(int queuePeekLimit) {
        _queuePeekLimit = queuePeekLimit;
    }

    public Duration getNotOwnerRetryDelay() {
        return _notOwnerRetryDelay;
    }

    public void setNotOwnerRetryDelay(Duration notOwnerRetryDelay) {
        _notOwnerRetryDelay = requireNonNull(notOwnerRetryDelay);
    }
}
