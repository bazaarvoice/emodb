package com.bazaarvoice.emodb.queue.core;

import com.bazaarvoice.emodb.event.api.EventStore;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.google.inject.Inject;

import java.time.Clock;

public class DefaultQueueService extends AbstractQueueService implements QueueService {
    @Inject
    public DefaultQueueService(EventStore eventStore, JobService jobService, JobHandlerRegistry jobHandlerRegistry,
                               Clock clock) {
        super(eventStore, jobService, jobHandlerRegistry, MoveQueueJob.INSTANCE, clock);
    }
}
