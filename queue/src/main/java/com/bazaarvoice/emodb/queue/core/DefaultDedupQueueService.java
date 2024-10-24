package com.bazaarvoice.emodb.queue.core;

import com.bazaarvoice.emodb.event.api.DedupEventStore;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.queue.api.DedupQueueService;
import com.bazaarvoice.emodb.queue.core.kafka.KafkaAdminService;
import com.bazaarvoice.emodb.queue.core.kafka.KafkaProducerService;
import com.bazaarvoice.emodb.queue.core.stepfn.StepFunctionService;
import com.google.inject.Inject;
import com.timgroup.statsd.StatsDClient;

import java.time.Clock;

public class DefaultDedupQueueService extends AbstractQueueService implements DedupQueueService {
    @Inject
    public DefaultDedupQueueService(DedupEventStore eventStore, JobService jobService, JobHandlerRegistry jobHandlerRegistry,
                                    Clock clock, KafkaAdminService adminService, KafkaProducerService producerService, StepFunctionService stepFunctionService, StatsDClient statsDClient) {
        super(eventStore, jobService, jobHandlerRegistry, MoveDedupQueueJob.INSTANCE, clock,adminService,producerService,stepFunctionService, statsDClient );
    }
}
