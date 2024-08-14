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
import com.bazaarvoice.emodb.sortedq.core.ReadOnlyQueueException;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

abstract class AbstractQueueService implements BaseQueueService {
    private final BaseEventStore _eventStore;
    private final JobService _jobService;
    private final JobType<MoveQueueRequest, MoveQueueResult> _moveQueueJobType;
    private final LoadingCache<SizeCacheKey, Map.Entry<Long, Long>> _queueSizeCache;
    private final Meter _sendAllMeterAQS;
    private final Meter _sendAllMeterNullAQS;

    private final Meter _pollAQS;
    private final Meter _pollNullAQS;

    public static final int MAX_MESSAGE_SIZE_IN_BYTES = 30 * 1024;

    protected AbstractQueueService(BaseEventStore eventStore, JobService jobService,
                                   JobHandlerRegistry jobHandlerRegistry,
                                   JobType<MoveQueueRequest, MoveQueueResult> moveQueueJobType,
                                   Clock clock,MetricRegistry metricRegistry) {
        _eventStore = eventStore;
        _jobService = jobService;
        _moveQueueJobType = moveQueueJobType;

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
        _sendAllMeterAQS = metricRegistry.meter(MetricRegistry.name(AbstractQueueService.class, "sendAllAQS"));
        _sendAllMeterNullAQS = metricRegistry.meter(MetricRegistry.name(AbstractQueueService.class, "sendAllNullAQS"));
        _pollAQS= metricRegistry.meter(MetricRegistry.name(AbstractQueueService.class,"pollNullAQS"));
        _pollNullAQS= metricRegistry.meter(MetricRegistry.name(AbstractQueueService.class,"pollNullAQS"));

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
        sendAll(Collections.singletonMap(queue, Collections.singleton(message)));
    }

    @Override
    public void sendAll(String queue, Collection<?> messages) {
        sendAll(Collections.singletonMap(queue, messages));
    }

    @Override
    public void sendAll(Map<String, ? extends Collection<?>> messagesByQueue) {
        requireNonNull(messagesByQueue, "messagesByQueue");
        if(messagesByQueue.keySet().isEmpty()){
            _sendAllMeterNullAQS.mark();
        } else {
            _sendAllMeterAQS.mark(messagesByQueue.keySet().size());
        }

        ImmutableMultimap.Builder<String, ByteBuffer> builder = ImmutableMultimap.builder();
        for (Map.Entry<String, ? extends Collection<?>> entry : messagesByQueue.entrySet()) {
            String queue = entry.getKey();
            Collection<?> messages = entry.getValue();

            checkLegalQueueName(queue);
            requireNonNull(messages, "messages");

            List<ByteBuffer> events = Lists.newArrayListWithCapacity(messages.size());
            for (Object message : messages) {
                ByteBuffer messageByteBuffer = MessageSerializer.toByteBuffer(JsonValidator.checkValid(message));
                checkArgument(messageByteBuffer.limit() <= MAX_MESSAGE_SIZE_IN_BYTES, "Message size (" + messageByteBuffer.limit() + ") is greater than the maximum allowed (" + MAX_MESSAGE_SIZE_IN_BYTES + ") message size");

                events.add(messageByteBuffer);
            }
            builder.putAll(queue, events);
        }
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
        List<Message> response = toMessages(_eventStore.poll(queue, claimTtl, limit));
        if(response.isEmpty()){
            _pollNullAQS.mark();
        }
        else{
            _pollAQS.mark(response.size());
        }
        return  response;
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
}
