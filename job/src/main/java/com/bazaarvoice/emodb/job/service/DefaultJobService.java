package com.bazaarvoice.emodb.job.service;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.job.JobZooKeeper;
import com.bazaarvoice.emodb.job.api.JobHandler;
import com.bazaarvoice.emodb.job.api.JobIdentifier;
import com.bazaarvoice.emodb.job.api.JobRequest;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.job.api.JobStatus;
import com.bazaarvoice.emodb.job.api.JobType;
import com.bazaarvoice.emodb.job.dao.JobStatusDAO;
import com.bazaarvoice.emodb.job.handler.JobHandlerRegistryInternal;
import com.bazaarvoice.emodb.job.handler.RegistryEntry;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Callables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.bazaarvoice.emodb.job.api.JobHandlerUtil.isNotOwner;
import static com.bazaarvoice.emodb.job.api.JobIdentifier.createNew;
import static com.bazaarvoice.emodb.job.api.JobIdentifier.fromString;
import static com.bazaarvoice.emodb.job.api.JobIdentifier.getJobTypeNameFromId;
import static com.bazaarvoice.emodb.job.util.JobStatusUtil.narrow;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class DefaultJobService implements JobService, Managed {

    private final static Callable<Instant> EPOCH = Callables.returning(Instant.EPOCH);

    private final Logger _log = LoggerFactory.getLogger(DefaultJobService.class);

    private final QueueService _queueService;
    private final String _queueName;
    private final JobHandlerRegistryInternal _jobHandlerRegistry;
    private final JobStatusDAO _jobStatusDAO;
    private final CuratorFramework _curator;
    private final int _concurrencyLevel;
    private final Duration _notOwnerRetryDelay;
    private final Supplier<Queue<Message>> _messageSupplier;
    private ScheduledExecutorService _service;
    private boolean _stopped = false;
    private final AtomicBoolean _paused = new AtomicBoolean(false);

    private final Cache<String, Instant> _recentNotOwnerDelays;

    @Inject
    public DefaultJobService(LifeCycleRegistry lifeCycleRegistry,
                             QueueService queueService,
                             @JobQueueName String queueName,
                             JobHandlerRegistryInternal jobHandlerRegistry,
                             JobStatusDAO jobStatusDAO,
                             @JobZooKeeper CuratorFramework curator,
                             @JobConcurrencyLevel Integer concurrencyLevel,
                             @QueueRefreshTime Duration queueRefreshTime,
                             final @QueuePeekLimit Integer queuePeekLimit,
                             @NotOwnerRetryDelay Duration notOwnerRetryDelay) {
        _queueService = checkNotNull(queueService, "queueService");
        _queueName = checkNotNull(queueName, "queueName");
        _jobHandlerRegistry = checkNotNull(jobHandlerRegistry, "jobHandlerRegistry");
        _jobStatusDAO = checkNotNull(jobStatusDAO, "jobStatusDAO");
        _curator = checkNotNull(curator, "curator");
        _concurrencyLevel = checkNotNull(concurrencyLevel, "concurrencyLevel");
        checkArgument(_concurrencyLevel >= 0, "Concurrency level cannot be negative");
        _notOwnerRetryDelay = checkNotNull(notOwnerRetryDelay, "notOwnerRetryDelay");
        checkNotNull(queuePeekLimit, "queuePeekLimit");
        checkNotNull(lifeCycleRegistry, "lifecycleRegistry");

        _recentNotOwnerDelays = CacheBuilder.newBuilder()
                .expireAfterWrite(notOwnerRetryDelay.toMillis(), TimeUnit.MILLISECONDS)
                .build();

        Supplier<Queue<Message>> sourceMessageSupplier = new Supplier<Queue<Message>>() {
            @Override
            public Queue<Message> get() {
                return Queues.synchronizedQueue(Queues.newArrayDeque(_queueService.peek(_queueName, queuePeekLimit)));
            }
        };

        checkNotNull(queueRefreshTime, "queueRefreshTime");
        if (queueRefreshTime.isZero()) {
            _messageSupplier = sourceMessageSupplier;
        } else {
            _messageSupplier = Suppliers.memoizeWithExpiration(
                    sourceMessageSupplier, queueRefreshTime.toMillis(), TimeUnit.MILLISECONDS);
        }

        lifeCycleRegistry.manage(this);
    }

    @Override
    public void start()
            throws Exception {
        if (_concurrencyLevel == 0) {
            _log.info("Job processing has been disabled");
            return;
        }

        _service = Executors.newScheduledThreadPool(_concurrencyLevel,
                new ThreadFactoryBuilder().setNameFormat("job-%d").build());

        // Schedule one thread for each level of concurrency

        Runnable drainQueue = new Runnable() {
            @Override
            public void run() {
                // Continue running until the job queue is empty or this service is stopped or paused
                while (!_stopped && !_paused.get()) {
                    boolean jobFound = runNextJob();
                    if (!jobFound) {
                        return;
                    }
                }
            }
        };

        // Schedule the actions which will process jobs on the queue until the queue is empty.
        // Whenever the queue is completely drained it will then sleep for 5 seconds before checking again.
        for (int i=0; i < _concurrencyLevel; i++) {
            _service.scheduleWithFixedDelay(drainQueue, 5, 5, TimeUnit.SECONDS);
        }
    }

    @Override
    public void stop()
            throws Exception {
        _stopped = true;
        if (_service != null) {
            _service.shutdownNow();
            _service = null;
        }
    }

    @Override
    public <Q, R> JobIdentifier<Q, R> submitJob(JobRequest<Q, R> jobRequest) {
        checkNotNull(jobRequest, "jobRequest");
        JobType<Q, R> jobType = jobRequest.getType();

        // Ensure there is a handler for this job type
        RegistryEntry<?, ?> entry = _jobHandlerRegistry.getRegistryEntry(jobType.getName());
        if (entry == null) {
            throw new IllegalArgumentException("Cannot handle job of type " + jobType);
        }

        // Create a unique job identifier
        JobIdentifier<Q, R> jobId = createNew(jobType);

        // Store the job status as "submitted"
        JobStatus<Q, R> jobStatus = new JobStatus<>(JobStatus.Status.SUBMITTED, jobRequest.getRequest(), null, null);
        _jobStatusDAO.updateJobStatus(jobId, jobStatus);

        // Queue the job
        _queueService.send(_queueName, jobId.toString());

        return jobId;
    }

    @Override
    public <Q, R> JobStatus<Q, R> getJobStatus(JobIdentifier<Q, R> id) {
        checkNotNull(id);
        JobStatus<?, ?> jobStatus = _jobStatusDAO.getJobStatus(id);
        if (jobStatus == null) {
            return null;
        }
        return narrow(jobStatus, id.getJobType());
    }

    /**
     * Dequeues the next job from the job queue and runs it.
     * @return True if a job was dequeued and executed, false if the queue was empty.
     */
    @VisibleForTesting
    boolean runNextJob() {
        try {
            Queue<Message> messages = _messageSupplier.get();
            Message message;

            while ((message = messages.poll()) != null) {
                String jobIdString = (String) message.getPayload();

                // If this job has recently reported that it cannot run on this server then skip it.
                Instant now = Instant.now();
                Instant delayUntilTime = _recentNotOwnerDelays.get(jobIdString, EPOCH);
                if (now.isBefore(delayUntilTime)) {
                    _log.debug("Waiting {} for next attempt to run job locally: {}",
                            Duration.between(now, delayUntilTime), jobIdString);
                    continue;
                }

                InterProcessMutex mutex = getMutex(jobIdString);

                if (!acquireMutex(mutex)) {
                    _log.debug("Failed to get mutex for job {}", jobIdString);
                    continue;
                }
                try {
                    String jobTypeName = getJobTypeNameFromId(jobIdString);
                    RegistryEntry<?, ?> entry = _jobHandlerRegistry.getRegistryEntry(jobTypeName);

                    _log.info("Executing job {}... ", jobIdString);

                    boolean ranLocally = run(jobIdString, entry);

                    if (ranLocally) {
                        acknowledgeQueueMessage(message.getId());
                        _log.info("Executing job {}... DONE", jobIdString);
                    } else {
                        // The job self-reported it could not be run locally.  Cache that knowledge and wait before
                        // attempting this job again.
                        _recentNotOwnerDelays.put(jobIdString, Instant.now().plus(_notOwnerRetryDelay));
                        _recentNotOwnerDelays.cleanUp();
                        _log.info("Executing job {}... not local", jobIdString);
                    }
                } finally {
                    mutex.release();
                }

                return true;
            }

            _log.debug("Job queue was empty or contained only non-local jobs");
        } catch (Throwable t) {
            _log.warn("runNextJob failed unexpectedly", t);
        }

        return false;
    }

    private InterProcessMutex getMutex(String jobId) {
        // TODO: use org.apache.curator.framework.recipes.locks.ChildReaper to cleanup nodes under /leader
        String path = format("/leader/%s", jobId);
        return new InterProcessMutex(_curator, path);
    }

    private boolean acquireMutex(InterProcessMutex mutex)
            throws Exception {
        return mutex.acquire(200, TimeUnit.MILLISECONDS);
    }

    private <Q, R> boolean run(String jobIdString, RegistryEntry<Q, R> registryEntry) {
        JobIdentifier<Q, R> jobId;
        Q request;

        // Load the job details and verify request is valid.
        try {
            if (registryEntry == null) {
                throw new IllegalArgumentException("No handler found for job type: " + getJobTypeNameFromId(jobIdString));
            }

            jobId = fromString(jobIdString, registryEntry.getJobType());
            JobStatus<Q, R> initialStatus = _jobStatusDAO.getJobStatus(jobId);

            // Verify the job exists
            if (initialStatus == null) {
                throw new IllegalArgumentException("Job not found: " + jobId);
            }

            request = initialStatus.getRequest();

            // Sanity check the job status.
            if (initialStatus.getStatus() != JobStatus.Status.SUBMITTED) {
                if (initialStatus.getStatus() == JobStatus.Status.RUNNING) {
                    // The use case here is that a server which was running the job terminated either while the
                    // job was running or without recording the final status.  Allow the job to be scheduled;
                    // the handler must be defensive against retries.
                    _log.info("Job failed previously for an unknown reason: [id={}, type={}]",
                            jobIdString, getJobTypeNameFromId(jobIdString));
                } else {
                    _log.info("Job has already run: [id={}, type={}]", jobIdString, getJobTypeNameFromId(jobIdString));
                    return true;
                }
            }
        } catch (Exception e) {
            _log.warn("Unable to execute job: [id={}, type={}]", jobIdString, getJobTypeNameFromId(jobIdString), e);
            return true;
        }

        try {
            // Update the status to note that this job is running
            _jobStatusDAO.updateJobStatus(jobId, new JobStatus<Q, R>(JobStatus.Status.RUNNING, request, null, null));

            // Get a handler to execute this job
            JobHandler<Q, R> handler = registryEntry.newHandler();

            // Execute the job
            R response = handler.run(request);

            if (isNotOwner(handler)) {
                // The job cannot run locally.  Set the status back to SUBMITTED to make it available on the server
                // which owns the job's resource.
                _jobStatusDAO.updateJobStatus(jobId, new JobStatus<Q, R>(JobStatus.Status.SUBMITTED, request, null, null));
                return false;
            }

            recordFinalStatus(jobId, new JobStatus<>(JobStatus.Status.FINISHED, request, response, null));
        } catch (Exception e) {
            _log.error("Job failed: [id={}, type={}]", jobId, jobId.getJobType(), e);
            recordFinalStatus(jobId, new JobStatus<Q, R>(JobStatus.Status.FAILED, request, null, e.getMessage()));
        }

        return true;
    }

    /**
     * Attempts to record the final status for a job.  Logs any errors, but always returns without throwing an
     * exception.
     * @param jobId The job ID
     * @param jobStatus The job's status
     * @param <Q> The job's request type
     * @param <R> The job's result type.
     */
    private <Q, R> void recordFinalStatus(JobIdentifier<Q, R> jobId, JobStatus<Q, R> jobStatus) {
        try {
            _jobStatusDAO.updateJobStatus(jobId, jobStatus);
        } catch (Exception e) {
            _log.error("Failed to record final status for job: [id={}, status={}]", jobId, jobStatus.getStatus(), e);
        }
    }

    /**
     * Attempts to acknowledge a message on the queue.  Logs any errors, but always returns without throwing an
     * exception.
     * @param messageId The message ID
     */
    private void acknowledgeQueueMessage(String messageId) {
        try {
            _queueService.acknowledge(_queueName, ImmutableList.of(messageId));
        } catch (Exception e) {
            _log.error("Failed to acknowledge message: [messageId={}]", messageId, e);
        }
    }

    @Override
    public boolean pause() {
        boolean stateChanged =  _paused.compareAndSet(false, true);
        if (stateChanged) {
            _log.info("Job processing has been paused");
        }
        return stateChanged;
    }

    @Override
    public boolean resume() {
        boolean stateChanged = _paused.compareAndSet(true, false);
        if (stateChanged) {
            _log.info("Job processing has been resumed");
        }
        return stateChanged;
    }
}
