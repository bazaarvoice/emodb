package com.bazaarvoice.emodb.queue.core;

import com.bazaarvoice.emodb.common.dropwizard.time.ClockTicker;
import com.bazaarvoice.emodb.common.json.JsonValidator;
import com.bazaarvoice.emodb.event.api.BaseEventStore;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.event.core.SizeCacheKey;
import com.bazaarvoice.emodb.job.api.JobHandler;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobIdentifier;
import com.bazaarvoice.emodb.job.api.JobRequest;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.job.api.JobStatus;
import com.bazaarvoice.emodb.job.api.JobType;
import com.bazaarvoice.emodb.queue.api.BaseQueueService;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.MoveQueueStatus;
import com.bazaarvoice.emodb.queue.api.Names;
import com.bazaarvoice.emodb.queue.api.UnknownMoveException;
import com.bazaarvoice.emodb.queue.core.kafka.KafkaAdminService;
import com.bazaarvoice.emodb.queue.core.kafka.KafkaProducerService;
import com.bazaarvoice.emodb.queue.core.ssm.ParameterStoreUtil;
import com.bazaarvoice.emodb.queue.core.stepfn.StepFunctionService;
import com.bazaarvoice.emodb.sortedq.core.ReadOnlyQueueException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractQueueService implements BaseQueueService {
    private final Logger _log = LoggerFactory.getLogger(AbstractQueueService.class);
    private final BaseEventStore _eventStore;
    private final JobService _jobService;
    private final JobType<MoveQueueRequest, MoveQueueResult> _moveQueueJobType;
    private final LoadingCache<SizeCacheKey, Map.Entry<Long, Long>> _queueSizeCache;
    private final KafkaAdminService adminService;
    private final KafkaProducerService producerService;

    public static final int MAX_MESSAGE_SIZE_IN_BYTES = 30 * 1024;
    private final StepFunctionService stepFunctionService;
    private final ParameterStoreUtil parameterStoreUtil;

    protected AbstractQueueService(BaseEventStore eventStore, JobService jobService,
                                   JobHandlerRegistry jobHandlerRegistry,
                                   JobType<MoveQueueRequest, MoveQueueResult> moveQueueJobType,
                                   Clock clock, KafkaAdminService adminService, KafkaProducerService producerService, StepFunctionService stepFunctionService) {
        _eventStore = eventStore;
        _jobService = jobService;
        _moveQueueJobType = moveQueueJobType;
        this.adminService = adminService;
        this.producerService = producerService;
        this.stepFunctionService = stepFunctionService;
        this.parameterStoreUtil = new ParameterStoreUtil();


        registerMoveQueueJobHandler(jobHandlerRegistry);
        _queueSizeCache = CacheBuilder.newBuilder()
                .expireAfterWrite(15, TimeUnit.SECONDS)
                .maximumSize(2000)
                .ticker(ClockTicker.getTicker(clock))
                .build(new CacheLoader<SizeCacheKey, Map.Entry<Long, Long>>() {
                    @Override
                    public Map.Entry<Long, Long> load(SizeCacheKey key)
                            throws Exception {
                        return Maps.immutableEntry(internalMessageCountUpTo(key.channelName, key.limitAsked), key.limitAsked);
                    }
                });
    }

    private void registerMoveQueueJobHandler(JobHandlerRegistry jobHandlerRegistry) {
        requireNonNull(jobHandlerRegistry, "jobHandlerRegistry");

        jobHandlerRegistry.addHandler(
                _moveQueueJobType,
                new Supplier<JobHandler<MoveQueueRequest, MoveQueueResult>>() {
                    @Override
                    public JobHandler<MoveQueueRequest, MoveQueueResult> get() {
                        return new JobHandler<MoveQueueRequest, MoveQueueResult>() {
                            @Override
                            public MoveQueueResult run(MoveQueueRequest request)
                                    throws Exception {
                                try {
                                    _eventStore.move(request.getFrom(), request.getTo());
                                } catch (ReadOnlyQueueException e) {
                                    // The from queue is not owned by this server.
                                    return notOwner();
                                }
                                return new MoveQueueResult(new Date());
                            }
                        };
                    }
                });
    }

    @Override
    public void send(String queue, Object message) {
        List<String> allowedQueues = fetchAllowedQueues();
        boolean isExperiment = Boolean.parseBoolean(parameterStoreUtil.getParameter("/emodb/experiment/isExperiment"));
        if (allowedQueues.contains(queue) && isExperiment) {
            // If queue is allowed and experiment is true , call the original sendAll method
            sendAll(Collections.singletonMap(queue, Collections.singleton(message)));
        } else {
            // Otherwise, call the alternative sendAll method with isExperiment flag
            sendAll(queue, Collections.singleton(message), isExperiment);
        }
    }

    @Override
    public void sendAll(String queue, Collection<?> messages) {
        List<String> allowedQueues = fetchAllowedQueues();
        boolean isExperiment = Boolean.parseBoolean(parameterStoreUtil.getParameter("/emodb/experiment/isExperiment"));
        if (allowedQueues.contains(queue) && isExperiment) {
            // If queue is allowed and experiment is true , call the original sendAll method
            sendAll(Collections.singletonMap(queue, messages));
        } else {
            // Otherwise, call the alternative sendAll method with isExperiment flag
            sendAll(queue, messages, isExperiment);
        }
    }


    private void validateMessage(Object message) {
        _log.debug("Validating message: {}", message);

        // Check if the message is valid using JsonValidator
        ByteBuffer messageByteBuffer = MessageSerializer.toByteBuffer(JsonValidator.checkValid(message));

        // Check if the message size exceeds the allowed limit
        checkArgument(messageByteBuffer.limit() <= MAX_MESSAGE_SIZE_IN_BYTES,
                "Message size (" + messageByteBuffer.limit() + ") is greater than the maximum allowed (" + MAX_MESSAGE_SIZE_IN_BYTES + ") message size");

        _log.debug("Message size is valid. Size: {}", messageByteBuffer.limit());
    }

    private void validateQueue(String queue, Collection<?> messages) {
        requireNonNull(queue, "Queue name cannot be null");
        requireNonNull(messages, "Messages collection cannot be null");

        // Check if the queue name is legal
        checkLegalQueueName(queue);

        _log.debug("Queue name '{}' is valid and contains {} messages", queue, messages.size());
    }

    @Override
    public void sendAll(Map<String, ? extends Collection<?>> messagesByQueue) {
        requireNonNull(messagesByQueue, "messagesByQueue");
        _log.info("Starting to send messages to queues. Total queues: {}", messagesByQueue.size());

        ImmutableMultimap.Builder<String, String> builder = ImmutableMultimap.builder();
        for (Map.Entry<String, ? extends Collection<?>> entry : messagesByQueue.entrySet()) {
            String queue = entry.getKey();
            Collection<?> messages = entry.getValue();

            _log.debug("Processing queue: {}", queue);
            // Validate the queue and messages
            validateQueue(queue, messages);

            List<Object> events = Lists.newArrayListWithCapacity(messages.size());
            _log.info("Processing {} messages for queue: {}", messages.size(), queue);

            // Validate each message
            for (Object message : messages) {
                validateMessage(message);
                events.add(message);
            }
            _log.info("Adding {} events to queue: {}", events.size(), queue);
            builder.putAll(queue, String.valueOf(events));
        }

        Multimap<String, String> eventsByChannel = builder.build();
        _log.info("Prepared {} channels to send messages.", eventsByChannel.asMap().size());


        String queueType = determineQueueType();
        for (Map.Entry<String, Collection<String>> topicEntry : eventsByChannel.asMap().entrySet()) {
            String topic = topicEntry.getKey();
            String queueName= topic;
            Collection<String> events = topicEntry.getValue();
            if ("dedup".equals(queueType)) {
                topic = "dedup_" + topic;
            }
            _log.debug("Sending {} messages to topic: {}", events.size(), topic);

            // Check if the topic exists, if not create it and execute Step Function
            if (!adminService.isTopicExists(topic)) {
                _log.info("Topic '{}' does not exist. Creating it now...", topic);
                adminService.createTopic(topic, 1, (short) 2, queueType);
                _log.info("Topic '{}' created.", topic);
                Map<String, String> parameters = fetchStepFunctionParameters();
                // Execute Step Function after topic creation
                executeStepFunction(parameters, queueType,queueName, topic);
            }
            producerService.sendMessages(topic, events, queueType);
            _log.info("Messages sent to topic: {}", topic);
        }
        _log.info("All messages have been sent to their respective queues.");
    }



    @Override
    public void sendAll(String queue, Collection<?> messages, boolean fromKafka) {
        //incoming message from kafka consume, save to cassandra

        if(!fromKafka){
            validateQueue(queue, messages);
        }
        ImmutableMultimap.Builder<String, ByteBuffer> builder = ImmutableMultimap.builder();
        List<ByteBuffer> events = Lists.newArrayListWithCapacity(messages.size());


        for (Object message : messages) {
            ByteBuffer messageByteBuffer = MessageSerializer.toByteBuffer(JsonValidator.checkValid(message));
            checkArgument(messageByteBuffer.limit() <= MAX_MESSAGE_SIZE_IN_BYTES,
                    "Message size (" + messageByteBuffer.limit() + ") is greater than the maximum allowed (" + MAX_MESSAGE_SIZE_IN_BYTES + ") message size");
            events.add(messageByteBuffer);
        }
        builder.putAll(queue, events);
        Multimap<String, ByteBuffer> eventsByChannel = builder.build();

        _eventStore.addAll(eventsByChannel);
    }

    @Override
    public long getMessageCount(String queue) {
        return getMessageCountUpTo(queue, Long.MAX_VALUE);
    }

    @Override
    public long getMessageCountUpTo(String queue, long limit) {
        // We get the size from cache as a tuple of size, and the limit used to estimate that size
        // So, the key is the size, and value is the limit used to estimate the size
        SizeCacheKey sizeCacheKey = new SizeCacheKey(queue, limit);
        Map.Entry<Long, Long> size = _queueSizeCache.getUnchecked(sizeCacheKey);
        if (size.getValue() >= limit) { // We have the same or better estimate
            return size.getKey();
        }
        // User wants a better estimate than what our cache has, so we need to invalidate this key and load a new size
        _queueSizeCache.invalidate(sizeCacheKey);
        return _queueSizeCache.getUnchecked(sizeCacheKey).getKey();
    }

    private long internalMessageCountUpTo(String queue, long limit) {
        checkLegalQueueName(queue);
        checkArgument(limit > 0, "Limit must be >0");

        return _eventStore.getSizeEstimate(queue, limit);
    }

    @Override
    public long getClaimCount(String queue) {
        checkLegalQueueName(queue);

        return _eventStore.getClaimCount(queue);
    }

    @Override
    public List<Message> peek(String queue, int limit) {
        checkLegalQueueName(queue);
        checkArgument(limit > 0, "Limit must be >0");

        return toMessages(_eventStore.peek(queue, limit));
    }

    @Override
    public List<Message> poll(String queue, Duration claimTtl, int limit) {
        checkLegalQueueName(queue);
        checkArgument(claimTtl.toMillis() >= 0, "ClaimTtl must be >=0");
        checkArgument(limit > 0, "Limit must be >0");

        return toMessages(_eventStore.poll(queue, claimTtl, limit));
    }

    @Override
    public void renew(String queue, Collection<String> messageIds, Duration claimTtl) {
        checkLegalQueueName(queue);
        requireNonNull(messageIds, "messageIds");
        checkArgument(claimTtl.toMillis() >= 0, "ClaimTtl must be >=0");

        _eventStore.renew(queue, messageIds, claimTtl, true);
    }

    @Override
    public void acknowledge(String queue, Collection<String> messageIds) {
        checkLegalQueueName(queue);
        requireNonNull(messageIds, "messageIds");

        _eventStore.delete(queue, messageIds, true);
    }

    @Override
    public String moveAsync(String from, String to) {
        checkLegalQueueName(from);
        checkLegalQueueName(to);

        JobIdentifier<MoveQueueRequest, MoveQueueResult> jobId =
                _jobService.submitJob(new JobRequest<>(_moveQueueJobType, new MoveQueueRequest(from, to)));

        return jobId.toString();
    }

    @Override
    public MoveQueueStatus getMoveStatus(String reference) {
        requireNonNull(reference, "reference");

        JobIdentifier<MoveQueueRequest, MoveQueueResult> jobId;
        try {
            jobId = JobIdentifier.fromString(reference, _moveQueueJobType);
        } catch (IllegalArgumentException e) {
            // The reference is illegal and therefore cannot match any move jobs.
            throw new UnknownMoveException(reference);
        }

        JobStatus<MoveQueueRequest, MoveQueueResult> status = _jobService.getJobStatus(jobId);

        if (status == null) {
            throw new UnknownMoveException(reference);
        }

        MoveQueueRequest request = status.getRequest();
        if (request == null) {
            throw new IllegalStateException("Move request details not found: " + jobId);
        }

        switch (status.getStatus()) {
            case FINISHED:
                return new MoveQueueStatus(request.getFrom(), request.getTo(), MoveQueueStatus.Status.COMPLETE);

            case FAILED:
                return new MoveQueueStatus(request.getFrom(), request.getTo(), MoveQueueStatus.Status.ERROR);

            default:
                return new MoveQueueStatus(request.getFrom(), request.getTo(), MoveQueueStatus.Status.IN_PROGRESS);
        }
    }

    @Override
    public void unclaimAll(String queue) {
        checkLegalQueueName(queue);

        _eventStore.unclaimAll(queue);
    }

    @Override
    public void purge(String queue) {
        checkLegalQueueName(queue);

        _eventStore.purge(queue);
    }

    private List<Message> toMessages(List<EventData> events) {
        return Lists.transform(events, new Function<EventData, Message>() {
            @Override
            public Message apply(EventData event) {
                return new Message(event.getId(), MessageSerializer.fromByteBuffer(event.getData()));
            }
        });
    }

    private void checkLegalQueueName(String queue) {
        checkArgument(Names.isLegalQueueName(queue),
                "Queue name must be a lowercase ASCII string between 1 and 255 characters in length. " +
                        "Allowed punctuation characters are -.:@_ and the queue name may not start with a single underscore character. " +
                        "An example of a valid table name would be 'polloi:provision'.");
    }
    /**
     * Fetches the necessary Step Function parameters from AWS Parameter Store.
     */
    private Map<String, String> fetchStepFunctionParameters() {
        List<String> parameterNames = Arrays.asList(
                "/emodb/stepfn/stateMachineArn",
                "/emodb/stepfn/queueThreshold",
                "/emodb/stepfn/batchSize",
                "/emodb/stepfn/interval"
        );

        try {
            return parameterStoreUtil.getParameters(parameterNames);
        } catch (Exception e) {
            _log.error("Failed to fetch Step Function parameters from Parameter Store", e);
            throw new RuntimeException("Error fetching Step Function parameters", e);
        }
    }
    /**
     * Executes the Step Function for a given topic after it has been created.
     */


    private void executeStepFunction(Map<String, String> parameters, String queueType, String queueName, String topic) {
        try {
            String stateMachineArn = parameters.get("/emodb/stepfn/stateMachineArn");
            int queueThreshold = Integer.parseInt(parameters.get("/emodb/stepfn/queueThreshold"));
            int batchSize = Integer.parseInt(parameters.get("/emodb/stepfn/batchSize"));
            int interval = Integer.parseInt(parameters.get("/emodb/stepfn/interval"));

            String inputPayload = createInputPayload(queueThreshold, batchSize, queueType, queueName, topic, interval);

            // Create the timestamp
            String timestamp = String.valueOf(System.currentTimeMillis()); // Current time in milliseconds

            // Check if queueType is "dedup" and prepend "D" to execution name if true
            String executionName = (queueType.equalsIgnoreCase("dedup") ? "D_" : "") + queueName + "_" + timestamp;

            // Start the Step Function execution
            stepFunctionService.startExecution(stateMachineArn, inputPayload, executionName);

            _log.info("Step Function executed for topic: {} with executionName: {}", topic, executionName);
        } catch (Exception e) {
            _log.error("Error executing Step Function for topic: {}", topic, e);
            throw new RuntimeException("Error executing Step Function for topic: " + topic, e);
        }
    }

    /**
     * Determines the queue type based on the event store.
     */
    private String determineQueueType() {
        if (_eventStore.getClass().getName().equals("com.bazaarvoice.emodb.event.dedup.DefaultDedupEventStore")) {
            return "dedup";
        }
        return "queue";
    }

    private List<String> fetchAllowedQueues() {
        try {
            // Fetch the 'allowedQueues' parameter using ParameterStoreUtil
            String allowedQueuesStr = parameterStoreUtil.getParameter("allowedQueues");
            return Arrays.asList(allowedQueuesStr.split(","));
        } catch (Exception e) {
            // Handle the case when the parameter is not found or fetching fails
            _log.error("Error fetching allowedQueues: " + e.getMessage());
            return Collections.singletonList("");  // Default to an empty list if the parameter is missing
        }
    }

    private String createInputPayload(int queueThreshold, int batchSize, String queueType,String queueName, String topicName, int interval) {
        Map<String, Object> payloadData = new HashMap<>();
        payloadData.put("queueThreshold", queueThreshold);
        payloadData.put("batchSize", batchSize);
        payloadData.put("queueType", queueType);
        payloadData.put("queueName",queueName);
        payloadData.put("topicName", topicName);
        payloadData.put("interval", interval);
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.writeValueAsString(payloadData);
        } catch (JsonProcessingException e) {
            _log.error("Error while converting map to JSON", e);
            return "{}"; // Return empty JSON object on error
        }
    }
}
