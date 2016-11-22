package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.time.ClockTicker;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.DefaultJoinFilter;
import com.bazaarvoice.emodb.databus.SystemInternalId;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Names;
import com.bazaarvoice.emodb.databus.api.ReplaySubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.api.UnauthorizedSubscriptionException;
import com.bazaarvoice.emodb.databus.api.UnknownMoveException;
import com.bazaarvoice.emodb.databus.api.UnknownReplayException;
import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;
import com.bazaarvoice.emodb.databus.auth.DatabusAuthorizer;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.event.api.EventSink;
import com.bazaarvoice.emodb.event.core.SizeCacheKey;
import com.bazaarvoice.emodb.job.api.JobHandler;
import com.bazaarvoice.emodb.job.api.JobHandlerRegistry;
import com.bazaarvoice.emodb.job.api.JobIdentifier;
import com.bazaarvoice.emodb.job.api.JobRequest;
import com.bazaarvoice.emodb.job.api.JobService;
import com.bazaarvoice.emodb.job.api.JobStatus;
import com.bazaarvoice.emodb.sor.api.Coordinate;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.core.UpdateIntentEvent;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.bazaarvoice.emodb.sortedq.core.ReadOnlyQueueException;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DefaultDatabus implements OwnerAwareDatabus, Managed {
    /** How long should poll loop, searching for events before giving up and returning. */
    private static final Duration MAX_POLL_TIME = Duration.millis(100);

    /** How long should the app wait before querying the data store again when it finds an unknown change? */
    private static final Duration RECENT_UNKNOWN_RETRY = Duration.millis(400);

    /** How old does an event need to be before we stop aggressively looking for it? */
    private static final Duration STALE_UNKNOWN_AGE = Duration.standardSeconds(2);

    /** Don't merge too many duplicate events together to avoid event keys getting unreasonably long. */
    private static final int MAX_EVENTS_TO_CONSOLIDATE = 1000;

    private final EventBus _eventBus;
    private final SubscriptionDAO _subscriptionDao;
    private final DatabusEventStore _eventStore;
    private final DataProvider _dataProvider;
    private final SubscriptionEvaluator _subscriptionEvaluator;
    private final JobService _jobService;
    private final DatabusAuthorizer _databusAuthorizer;
    private final String _systemOwnerId;
    private final Meter _peekedMeter;
    private final Meter _polledMeter;
    private final Meter _renewedMeter;
    private final Meter _ackedMeter;
    private final Meter _recentUnknownMeter;
    private final Meter _staleUnknownMeter;
    private final Meter _redundantMeter;
    private final Meter _discardedMeter;
    private final Meter _consolidatedMeter;
    private final Meter _unownedSubscriptionMeter;
    private final LoadingCache<SizeCacheKey, Map.Entry<Long, Long>> _eventSizeCache;
    private final Supplier<Condition> _defaultJoinFilterCondition;
    private final Ticker _ticker;
    private final Clock _clock;

    @Inject
    public DefaultDatabus(LifeCycleRegistry lifeCycle, EventBus eventBus, DataProvider dataProvider,
                          SubscriptionDAO subscriptionDao, DatabusEventStore eventStore,
                          SubscriptionEvaluator subscriptionEvaluator, JobService jobService,
                          JobHandlerRegistry jobHandlerRegistry, DatabusAuthorizer databusAuthorizer,
                          @SystemInternalId String systemOwnerId,
                          @DefaultJoinFilter Supplier<Condition> defaultJoinFilterCondition,
                          MetricRegistry metricRegistry, Clock clock) {
        _eventBus = eventBus;
        _subscriptionDao = subscriptionDao;
        _eventStore = eventStore;
        _dataProvider = dataProvider;
        _subscriptionEvaluator = subscriptionEvaluator;
        _jobService = jobService;
        _databusAuthorizer = databusAuthorizer;
        _systemOwnerId = systemOwnerId;
        _defaultJoinFilterCondition = defaultJoinFilterCondition;
        _ticker = ClockTicker.getTicker(clock);
        _clock = clock;
        _peekedMeter = newEventMeter("peeked", metricRegistry);
        _polledMeter = newEventMeter("polled", metricRegistry);
        _renewedMeter = newEventMeter("renewed", metricRegistry);
        _ackedMeter = newEventMeter("acked", metricRegistry);
        _recentUnknownMeter = newEventMeter("recent-unknown", metricRegistry);
        _staleUnknownMeter = newEventMeter("stale-unknown", metricRegistry);
        _redundantMeter = newEventMeter("redundant", metricRegistry);
        _discardedMeter = newEventMeter("discarded", metricRegistry);
        _consolidatedMeter = newEventMeter("consolidated", metricRegistry);
        _unownedSubscriptionMeter = newEventMeter("unowned", metricRegistry);
        _eventSizeCache = CacheBuilder.newBuilder()
                .expireAfterWrite(15, TimeUnit.SECONDS)
                .maximumSize(2000)
                .ticker(_ticker)
                .build(new CacheLoader<SizeCacheKey, Map.Entry<Long, Long>>() {
                    @Override
                    public Map.Entry<Long, Long> load(SizeCacheKey key)
                            throws Exception {
                        return Maps.immutableEntry(internalEventCountUpTo(key.channelName, key.limitAsked), key.limitAsked);
                    }
                });
        lifeCycle.manage(this);

        checkNotNull(jobHandlerRegistry, "jobHandlerRegistry");
        registerMoveSubscriptionJobHandler(jobHandlerRegistry);
        registerReplaySubscriptionJobHandler(jobHandlerRegistry);
    }

    private void registerMoveSubscriptionJobHandler(JobHandlerRegistry jobHandlerRegistry) {
        jobHandlerRegistry.addHandler(
                MoveSubscriptionJob.INSTANCE,
                new Supplier<JobHandler<MoveSubscriptionRequest, MoveSubscriptionResult>>() {
                    @Override
                    public JobHandler<MoveSubscriptionRequest, MoveSubscriptionResult> get() {
                        return new JobHandler<MoveSubscriptionRequest, MoveSubscriptionResult>() {
                            @Override
                            public MoveSubscriptionResult run(MoveSubscriptionRequest request)
                                    throws Exception {
                                try {
                                    // Last chance to verify the subscriptions' owner before doing anything mutative
                                    checkSubscriptionOwner(request.getOwnerId(), request.getFrom());
                                    checkSubscriptionOwner(request.getOwnerId(), request.getTo());

                                    _eventStore.move(request.getFrom(), request.getTo());
                                } catch (ReadOnlyQueueException e) {
                                    // The from queue is not owned by this server.
                                    return notOwner();
                                }
                                return new MoveSubscriptionResult(new Date());
                            }
                        };
                    }
                });
    }

    private void registerReplaySubscriptionJobHandler(JobHandlerRegistry jobHandlerRegistry) {
        jobHandlerRegistry.addHandler(
                ReplaySubscriptionJob.INSTANCE,
                new Supplier<JobHandler<ReplaySubscriptionRequest, ReplaySubscriptionResult>>() {
                    @Override
                    public JobHandler<ReplaySubscriptionRequest, ReplaySubscriptionResult> get() {
                        return new JobHandler<ReplaySubscriptionRequest, ReplaySubscriptionResult>() {
                            @Override
                            public ReplaySubscriptionResult run(ReplaySubscriptionRequest request)
                                    throws Exception {
                                try {
                                    // Last chance to verify the subscription's owner before doing anything mutative
                                    checkSubscriptionOwner(request.getOwnerId(), request.getSubscription());

                                    replay(request.getSubscription(), request.getSince());
                                } catch (ReadOnlyQueueException e) {
                                    // The subscription is not owned by this server.
                                    return notOwner();
                                }
                                // Make sure that we have completed the replay request in time.
                                // If replay took more than the replay TTL time, we should fail it as
                                // there could be some events that were expired before we could replay it
                                if (request.getSince() != null && new DateTime(request.getSince())
                                        .plus(DatabusChannelConfiguration.REPLAY_TTL)
                                        .isBeforeNow()) {
                                    // Uh-oh we were too late in replaying, and could have lost some events
                                    throw new ReplayTooLateException();
                                }
                                return new ReplaySubscriptionResult(new Date());
                            }
                        };
                    }
                });
    }

    private void createDatabusReplaySubscription() {
        // Create a master databus replay subscription where the events expire every 50 hours (2 days + 2 hours)
        subscribe(_systemOwnerId, ChannelNames.getMasterReplayChannel(), Conditions.alwaysTrue(),
                Duration.standardDays(3650), DatabusChannelConfiguration.REPLAY_TTL, false);
    }

    private Meter newEventMeter(String name, MetricRegistry metricRegistry) {
        return metricRegistry.meter(getMetricName(name));
    }

    private String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.databus", "DefaultDatabus", name);
    }

    @Override
    public void start() throws Exception {
        // Create a databus replay subscription
        createDatabusReplaySubscription();
        _eventBus.register(this);
    }

    @Override
    public void stop() throws Exception {
        _eventBus.unregister(this);
    }

    @Override
    public Iterator<Subscription> listSubscriptions(final String ownerId, @Nullable String fromSubscriptionExclusive, long limit) {
        checkArgument(limit > 0, "Limit must be >0");

        // We always have all the subscriptions cached in memory so fetch them all.
        Iterable<OwnedSubscription> allSubscriptions = _subscriptionDao.getAllSubscriptions();

        return StreamSupport.stream(allSubscriptions.spliterator(), false)
                // Ignore subscriptions not accessible by the owner.
                .filter((subscription) -> _databusAuthorizer.owner(ownerId).canAccessSubscription(subscription))
                // Sort them by name.  They're stored sorted in Cassandra so this should be a no-op, but
                // do the sort anyway so we're not depending on internals of the subscription DAO.
                .sorted((left, right) -> left.getName().compareTo(right.getName()))
                // Apply the "from" parameter
                .filter(subscription -> fromSubscriptionExclusive == null || subscription.getName().compareTo(fromSubscriptionExclusive) > 0)
                // Apply the "limit" parameter (be careful to avoid overflow when limit == Long.MAX_VALUE).
                .limit(limit)
                // Necessary to make generics work
                .map(subscription -> (Subscription) subscription)
                .iterator();
    }

    @Override
    public void subscribe(String ownerId, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl) {
        subscribe(ownerId, subscription, tableFilter, subscriptionTtl, eventTtl, true);
    }

    @Override
    public void subscribe(String ownerId, String subscription, Condition tableFilter, Duration subscriptionTtl,
                          Duration eventTtl, boolean includeDefaultJoinFilter) {
        // This call should be deprecated soon.
        checkLegalSubscriptionName(subscription);
        checkSubscriptionOwner(ownerId, subscription);
        checkNotNull(tableFilter, "tableFilter");
        checkArgument(subscriptionTtl.isLongerThan(Duration.ZERO), "SubscriptionTtl must be >0");
        checkArgument(eventTtl.isLongerThan(Duration.ZERO), "EventTtl must be >0");
        TableFilterValidator.checkAllowed(tableFilter);

        if (includeDefaultJoinFilter) {
            // If the default join filter condition is set (that is, isn't "alwaysTrue()") then add it to the filter
            Condition defaultJoinFilterCondition = _defaultJoinFilterCondition.get();
            if (!Conditions.alwaysTrue().equals(defaultJoinFilterCondition)) {
                if (tableFilter.equals(Conditions.alwaysTrue())) {
                    tableFilter = defaultJoinFilterCondition;
                } else {
                    tableFilter = Conditions.and(tableFilter, defaultJoinFilterCondition);
                }
            }
        }

        // except for resetting the ttl, recreating a subscription that already exists has no effect.
        // assume that multiple servers that manage the same subscriptions can each attempt to create
        // the subscription at startup.
        _subscriptionDao.insertSubscription(ownerId, subscription, tableFilter, subscriptionTtl, eventTtl);
    }

    @Override
    public void unsubscribe(String ownerId, String subscription) {
        checkLegalSubscriptionName(subscription);
        checkSubscriptionOwner(ownerId, subscription);

        _subscriptionDao.deleteSubscription(subscription);
        _eventStore.purge(subscription);
    }

    @Override
    public Subscription getSubscription(String ownerId, String name) throws UnknownSubscriptionException {
        checkLegalSubscriptionName(name);

        OwnedSubscription subscription = getSubscriptionByName(name);
        checkSubscriptionOwner(ownerId, subscription);

        return subscription;
    }

    private OwnedSubscription getSubscriptionByName(String name) {
        OwnedSubscription subscription = _subscriptionDao.getSubscription(name);
        if (subscription == null) {
            throw new UnknownSubscriptionException(name);
        }
        return subscription;
    }

    @Subscribe
    public void onUpdateIntent(UpdateIntentEvent event) {
        List<ByteBuffer> eventIds = Lists.newArrayListWithCapacity(event.getUpdateRefs().size());
        for (UpdateRef ref : event.getUpdateRefs()) {
            eventIds.add(UpdateRefSerializer.toByteBuffer(ref));
        }
        _eventStore.addAll(ChannelNames.getMasterFanoutChannel(), eventIds);
    }

    @Override
    public long getEventCount(String ownerId, String subscription) {
        return getEventCountUpTo(ownerId, subscription, Long.MAX_VALUE);
    }

    @Override
    public long getEventCountUpTo(String ownerId, String subscription, long limit) {
        checkSubscriptionOwner(ownerId, subscription);

        // We get the size from cache as a tuple of size, and the limit used to estimate that size
        // So, the key is the size, and value is the limit used to estimate the size
        SizeCacheKey sizeCacheKey = new SizeCacheKey(subscription, limit);
        Map.Entry<Long, Long> size = _eventSizeCache.getUnchecked(sizeCacheKey);
        if (size.getValue() >= limit) { // We have the same or better estimate
            return size.getKey();
        }
        // User wants a better estimate than what our cache has, so we need to invalidate this key and load a new size
        _eventSizeCache.invalidate(sizeCacheKey);
        return _eventSizeCache.getUnchecked(sizeCacheKey).getKey();
    }

    private long internalEventCountUpTo(String subscription, long limit) {
        checkLegalSubscriptionName(subscription);
        checkArgument(limit > 0, "Limit must be >0");

        return _eventStore.getSizeEstimate(subscription, limit);
    }

    @Override
    public long getClaimCount(String ownerId, String subscription) {
        checkLegalSubscriptionName(subscription);
        checkSubscriptionOwner(ownerId, subscription);

        return _eventStore.getClaimCount(subscription);
    }

    @Override
    public List<Event> peek(String ownerId, final String subscription, int limit) {
        checkLegalSubscriptionName(subscription);
        checkArgument(limit > 0, "Limit must be >0");
        checkSubscriptionOwner(ownerId, subscription);

        List<Event> events = peekOrPoll(subscription, null, limit);
        _peekedMeter.mark(events.size());
        return events;
    }

    @Override
    public List<Event> poll(String ownerId, final String subscription, final Duration claimTtl, int limit) {
        checkLegalSubscriptionName(subscription);
        checkArgument(claimTtl.getMillis() >= 0, "ClaimTtl must be >=0");
        checkArgument(limit > 0, "Limit must be >0");
        checkSubscriptionOwner(ownerId, subscription);

        List<Event> events = peekOrPoll(subscription, claimTtl, limit);
        _polledMeter.mark(events.size());
        return events;
    }

    /** Implements peek() or poll() based on whether claimTtl is null or non-null. */
    private List<Event> peekOrPoll(String subscription, @Nullable Duration claimTtl, int limit) {
        List<Item> items = Lists.newArrayList();
        Map<Coordinate, Item> uniqueItems = Maps.newHashMap();
        Map<Coordinate, Integer> eventOrder = Maps.newHashMap();
        boolean repeatable = claimTtl != null && claimTtl.getMillis() > 0;

        Stopwatch stopwatch = Stopwatch.createStarted(_ticker);
        int padding = 0;
        do {
            int remaining = limit - items.size();
            if (remaining == 0) {
                break;  // Don't need any more events.
            }

            // Query the databus event store.  Consolidate multiple events that refer to the same item.
            ConsolidatingEventSink sink = new ConsolidatingEventSink(remaining + padding);
            boolean more = (claimTtl == null) ?
                    _eventStore.peek(subscription, sink) :
                    _eventStore.poll(subscription, claimTtl, sink);
            Map<Coordinate, EventList> rawEvents = sink.getEvents();

            if (rawEvents.isEmpty()) {
                break;  // No events to be had.
            }

            List<String> eventIdsToDiscard = Lists.newArrayList();
            List<String> recentUnknownEventIds = Lists.newArrayList();
            List<String> eventIdsToUnclaim = Lists.newArrayList();

            // Query the events from the data store in batch to reduce latency.
            DataProvider.AnnotatedGet annotatedGet = _dataProvider.prepareGetAnnotated(ReadConsistency.STRONG);
            for (Map.Entry<Coordinate, EventList> entry : rawEvents.entrySet()) {
                Coordinate coord = entry.getKey();

                // Query the table/key pair.
                try {
                    annotatedGet.add(coord.getTable(), coord.getId());
                } catch (UnknownTableException e) {
                    // It's likely the table was dropped since the event was queued.  Discard the events.
                    EventList list = entry.getValue();
                    for (Pair<String, UUID> pair : list.getEventAndChangeIds()) {
                        eventIdsToDiscard.add(pair.first());
                    }
                    _discardedMeter.mark(list.size());
                }

                // Keep track of the order in which we received the events from the EventStore.
                if (!eventOrder.containsKey(coord)) {
                    eventOrder.put(coord, eventOrder.size());
                }
            }
            Iterator<DataProvider.AnnotatedContent> readResultIter = annotatedGet.execute();

            // Loop through the results of the data store query.
            while (readResultIter.hasNext()) {
                DataProvider.AnnotatedContent readResult = readResultIter.next();

                // Get the JSON System of Record entity for this piece of content
                Map<String, Object> content = readResult.getContent();

                // Find the original event IDs that correspond to this piece of content
                Coordinate coord = Coordinate.fromJson(content);
                EventList eventList = rawEvents.get(coord);

                // Get all databus event tags for the original event(s) for this coordinate
                List<List<String>> tags = eventList.getTags();

                // Loop over the Databus events for this piece of content.  Usually there's just one, but not always...
                for (Pair<String, UUID> eventData : eventList.getEventAndChangeIds()) {
                    String eventId = eventData.first();
                    UUID changeId = eventData.second();

                    // Has the content replicated yet?  If not, abandon the event and we'll try again when the claim expires.
                    if (readResult.isChangeDeltaPending(changeId)) {
                        if (isRecent(changeId)) {
                            recentUnknownEventIds.add(eventId);
                            _recentUnknownMeter.mark();
                        } else {
                            _staleUnknownMeter.mark();
                        }
                        continue;
                    }

                    // Is the change redundant?  If so, no need to fire databus events for it.  Ack it now.
                    if (readResult.isChangeDeltaRedundant(changeId)) {
                        eventIdsToDiscard.add(eventId);
                        _redundantMeter.mark();
                        continue;
                    }

                    // Check whether we've already added this piece of content to the poll result.  If so, consolidate
                    // the two together to reduce the amount of work a client must do.  Note that the previous item
                    // might be from a previous batch of events and it's possible that we have read two different
                    // versions of the same item of content.  This will prefer the most recent.
                    Item previousItem = uniqueItems.get(coord);
                    if (previousItem != null && previousItem.consolidateWith(eventId, content, tags)) {
                        _consolidatedMeter.mark();
                        continue;
                    }

                    // If, due to "padding" we asked for too many events, release the claims for the next poll().
                    if (items.size() == limit) {
                        eventIdsToUnclaim.add(eventId);
                        continue;
                    }

                    // We have found a new item of content to return!
                    Item item = new Item(eventId, eventOrder.get(coord), content, tags);
                    items.add(item);
                    uniqueItems.put(coord, item);
                }
            }

            // Abandon claims when we claimed more items than necessary to satisfy the specified limit.
            if (!eventIdsToUnclaim.isEmpty()) {
                _eventStore.renew(subscription, eventIdsToUnclaim, Duration.ZERO, false);
            }
            // Reduce the claim length on recent unknown IDs so we look for them again soon.
            if (!recentUnknownEventIds.isEmpty()) {
                _eventStore.renew(subscription, recentUnknownEventIds, RECENT_UNKNOWN_RETRY, false);
            }
            // Ack events we never again want to see.
            if (!eventIdsToDiscard.isEmpty()) {
                _eventStore.delete(subscription, eventIdsToDiscard, true);
            }
            if (!more) {
                break;  // Didn't get a full batch, that means there are no more events to be had.
            }

            // Note: Due to redundant/unknown events, it's possible that the 'events' list is empty even though, if we
            // tried again, we'd find more events.  Try again a few times, but not for more than 250ms so clients don't
            // timeout the request.  This helps move through large amounts of redundant deltas relatively quickly while
            // also putting a bound on the total amount of work done by a single call to poll().
            padding = 10;
        } while (repeatable && stopwatch.elapsed(TimeUnit.MILLISECONDS) <  MAX_POLL_TIME.getMillis());

        // Sort the items to match the order of their events in an attempt to get first-in-first-out.
        Collections.sort(items);

        // Return the final list of events.
        List<Event> events = Lists.newArrayListWithCapacity(items.size());
        for (Item item : items) {
            events.add(item.toEvent());
        }
        return events;
    }

    private boolean isRecent(UUID changeId) {
        return _clock.millis() - TimeUUIDs.getTimeMillis(changeId) < STALE_UNKNOWN_AGE.getMillis();
    }

    @Override
    public void renew(String ownerId, String subscription, Collection<String> eventKeys, Duration claimTtl) {
        checkLegalSubscriptionName(subscription);
        checkNotNull(eventKeys, "eventKeys");
        checkArgument(claimTtl.getMillis() >= 0, "ClaimTtl must be >=0");
        checkSubscriptionOwner(ownerId, subscription);

        _eventStore.renew(subscription, EventKeyFormat.decodeAll(eventKeys), claimTtl, true);
        _renewedMeter.mark(eventKeys.size());
    }

    @Override
    public void acknowledge(String ownerId, String subscription, Collection<String> eventKeys) {
        checkLegalSubscriptionName(subscription);
        checkNotNull(eventKeys, "eventKeys");
        checkSubscriptionOwner(ownerId, subscription);

        _eventStore.delete(subscription, EventKeyFormat.decodeAll(eventKeys), true);
        _ackedMeter.mark(eventKeys.size());
    }

    @Override
    public String replayAsync(String ownerId, String subscription) {
        return replayAsyncSince(ownerId, subscription, null);
    }

    @Override
    public String replayAsyncSince(String ownerId, String subscription, Date since) {
        checkLegalSubscriptionName(subscription);
        checkSubscriptionOwner(ownerId, subscription);

        JobIdentifier<ReplaySubscriptionRequest, ReplaySubscriptionResult> jobId =
                _jobService.submitJob(
                        new JobRequest<>(ReplaySubscriptionJob.INSTANCE, new ReplaySubscriptionRequest(ownerId, subscription, since)));

        return jobId.toString();
    }

    public void replay(String subscription, Date since) {
        // Make sure since is within Replay TTL
        checkState(since == null || new DateTime(since).plus(DatabusChannelConfiguration.REPLAY_TTL).isAfterNow(),
                "Since timestamp is outside the replay TTL.");
        String source = ChannelNames.getMasterReplayChannel();
        final OwnedSubscription destination = getSubscriptionByName(subscription);

        _eventStore.copy(source, subscription,
                (eventDataBytes) -> _subscriptionEvaluator.matches(destination, eventDataBytes),
                since);
    }

    @Override
    public ReplaySubscriptionStatus getReplayStatus(String ownerId, String reference) {
        checkNotNull(reference, "reference");

        JobIdentifier<ReplaySubscriptionRequest, ReplaySubscriptionResult> jobId;
        try {
            jobId = JobIdentifier.fromString(reference, ReplaySubscriptionJob.INSTANCE);
        } catch (IllegalArgumentException e) {
            // The reference is illegal and therefore cannot match any replay jobs.
            throw new UnknownReplayException(reference);
        }

        JobStatus<ReplaySubscriptionRequest, ReplaySubscriptionResult> status = _jobService.getJobStatus(jobId);

        if (status == null) {
            throw new UnknownReplayException(reference);
        }

        ReplaySubscriptionRequest request = status.getRequest();
        if (request == null) {
            throw new IllegalStateException("Replay request details not found: " + jobId);
        }

        checkSubscriptionOwner(ownerId, request.getSubscription());

        switch (status.getStatus()) {
            case FINISHED:
                return new ReplaySubscriptionStatus(request.getSubscription(), ReplaySubscriptionStatus.Status.COMPLETE);

            case FAILED:
                return new ReplaySubscriptionStatus(request.getSubscription(), ReplaySubscriptionStatus.Status.ERROR);

            default:
                return new ReplaySubscriptionStatus(request.getSubscription(), ReplaySubscriptionStatus.Status.IN_PROGRESS);
        }
    }

    @Override
    public String moveAsync(String ownerId, String from, String to) {
        checkLegalSubscriptionName(from);
        checkLegalSubscriptionName(to);
        checkSubscriptionOwner(ownerId, from);
        checkSubscriptionOwner(ownerId, to);

        JobIdentifier<MoveSubscriptionRequest, MoveSubscriptionResult> jobId =
                _jobService.submitJob(new JobRequest<>(
                        MoveSubscriptionJob.INSTANCE, new MoveSubscriptionRequest(ownerId, from, to)));

        return jobId.toString();
    }

    @Override
    public MoveSubscriptionStatus getMoveStatus(String ownerId, String reference) {
        checkNotNull(reference, "reference");

        JobIdentifier<MoveSubscriptionRequest, MoveSubscriptionResult> jobId;
        try {
            jobId = JobIdentifier.fromString(reference, MoveSubscriptionJob.INSTANCE);
        } catch (IllegalArgumentException e) {
            // The reference is illegal and therefore cannot match any move jobs.
            throw new UnknownMoveException(reference);
        }

        JobStatus<MoveSubscriptionRequest, MoveSubscriptionResult> status = _jobService.getJobStatus(jobId);

        if (status == null) {
            throw new UnknownMoveException(reference);
        }

        MoveSubscriptionRequest request = status.getRequest();
        if (request == null) {
            throw new IllegalStateException("Move request details not found: " + jobId);
        }

        checkSubscriptionOwner(ownerId, request.getFrom());

        switch (status.getStatus()) {
            case FINISHED:
                return new MoveSubscriptionStatus(request.getFrom(), request.getTo(), MoveSubscriptionStatus.Status.COMPLETE);

            case FAILED:
                return new MoveSubscriptionStatus(request.getFrom(), request.getTo(), MoveSubscriptionStatus.Status.ERROR);

            default:
                return new MoveSubscriptionStatus(request.getFrom(), request.getTo(), MoveSubscriptionStatus.Status.IN_PROGRESS);
        }
    }

    @Override
    public void injectEvent(String ownerId, String subscription, String table, String key) {
        // Pick a changeId UUID that's guaranteed to be older than the compaction cutoff so poll()'s calls to
        // AnnotatedContent.isChangeDeltaPending() and isChangeDeltaRedundant() will always return false.
        checkSubscriptionOwner(ownerId, subscription);
        UpdateRef ref = new UpdateRef(table, key, TimeUUIDs.minimumUuid(), ImmutableSet.<String>of());
        _eventStore.add(subscription, UpdateRefSerializer.toByteBuffer(ref));
    }

    @Override
    public void unclaimAll(String ownerId, String subscription) {
        checkLegalSubscriptionName(subscription);
        checkSubscriptionOwner(ownerId, subscription);

        _eventStore.unclaimAll(subscription);
    }

    @Override
    public void purge(String ownerId, String subscription) {
        checkLegalSubscriptionName(subscription);
        checkSubscriptionOwner(ownerId, subscription);

        _eventStore.purge(subscription);
    }

    private void checkLegalSubscriptionName(String subscription) {
        checkArgument(Names.isLegalSubscriptionName(subscription),
                "Subscription name must be a lowercase ASCII string between 1 and 255 characters in length. " +
                        "Allowed punctuation characters are -.:@_ and the subscription name may not start with a single underscore character. " +
                        "An example of a valid subscription name would be 'polloi:review'.");
    }

    private void checkSubscriptionOwner(String ownerId, String subscription) {
        // Verify the subscription either doesn't exist or is already owned by the same owner.  In practice this is
        // predominantly cached by SubscriptionDAO so performance should be good.
        checkSubscriptionOwner(ownerId, _subscriptionDao.getSubscription(subscription));
    }

    private void checkSubscriptionOwner(String ownerId, OwnedSubscription subscription) {
        checkNotNull(ownerId, "ownerId");
        if (subscription != null) {
            // Grandfather-in subscriptions created before ownership was introduced.  This should be a temporary issue
            // since the subscriptions will need to renew at some point or expire.
            if (subscription.getOwnerId() == null) {
                _unownedSubscriptionMeter.mark();
            } else if (!_databusAuthorizer.owner(ownerId).canAccessSubscription(subscription)) {
                throw new UnauthorizedSubscriptionException("Not subscriber", subscription.getName());
            }
        }
    }

    /** EventStore sink that doesn't count adjacent events for the same table/key against the peek/poll limit. */
    private class ConsolidatingEventSink implements EventSink {
        private final Map<Coordinate, EventList> _eventMap = Maps.newLinkedHashMap();
        private final int _limit;

        ConsolidatingEventSink(int limit) {
            _limit = limit;
        }

        @Override
        public int remaining() {
            // Go a bit past the desired limit to maximize opportunity to consolidate events.  Otherwise, for example,
            // if limit was 1, we'd stop after the first event and not consolidate anything.
            return _limit - _eventMap.size() + 1;
        }

        @Override
        public Status accept(EventData rawEvent) {
            // Parse the raw event data into table/key/changeId/tags.
            UpdateRef ref = UpdateRefSerializer.fromByteBuffer(rawEvent.getData());

            // Consolidate events that refer to the same item.
            Coordinate contentKey = Coordinate.of(ref.getTable(), ref.getKey());
            EventList eventList = _eventMap.get(contentKey);
            if (eventList == null) {
                if (_eventMap.size() == _limit) {
                    return Status.REJECTED_STOP;
                }
                _eventMap.put(contentKey, eventList = new EventList());
            }

            eventList.add(rawEvent.getId(), ref.getChangeId(), ref.getTags());

            if (eventList.size() == MAX_EVENTS_TO_CONSOLIDATE) {
                return Status.ACCEPTED_STOP;
            }

            return Status.ACCEPTED_CONTINUE;
        }

        Map<Coordinate, EventList> getEvents() {
            return _eventMap;
        }
    }

    private static class EventList {
        private final List<Pair<String, UUID>> _eventAndChangeIds = Lists.newArrayList();
        private List<List<String>> _tags;

        void add(String eventId, UUID changeId, Set<String> tags) {
            _eventAndChangeIds.add(Pair.of(eventId, changeId));
            _tags = sortedTagUnion(_tags, tags);
        }

        List<Pair<String, UUID>> getEventAndChangeIds() {
            return _eventAndChangeIds;
        }

        List<List<String>> getTags() {
            return _tags;
        }

        int size() {
            return _eventAndChangeIds.size();
        }
    }

    private static class Item implements Comparable<Item> {
        private final List<String> _consolidatedEventIds;
        private final int _sortIndex;
        private Map<String, Object> _content;
        private List<List<String>> _tags;

        Item(String eventId, int sortIndex, Map<String, Object> content, List<List<String>> tags) {
            _consolidatedEventIds = Lists.newArrayList(eventId);
            _sortIndex = sortIndex;
            _content = content;
            _tags = tags;
        }

        boolean consolidateWith(String eventId, Map<String, Object> content, List<List<String>> tags) {
            if (_consolidatedEventIds.size() >= MAX_EVENTS_TO_CONSOLIDATE) {
                return false;
            }

            // We'll construct an event key that combines all the duplicate events.  Unfortunately we can't discard/ack
            // the extra events at this time, subtle race conditions result that can cause clients to miss events.
            _consolidatedEventIds.add(eventId);

            // Pick the newest version of the content.  There's no reason to return stale stuff.
            if (Intrinsic.getVersion(_content) < Intrinsic.getVersion(content)) {
                _content = content;
            }

            // Combine tags from the other event
            for (List<String> tagList : tags) {
                // The contents of "tagList" are already sorted, no need to sort again.
                _tags = sortedTagUnion(_tags, tagList);
            }

            return true;
        }

        Event toEvent() {
            Collections.sort(_consolidatedEventIds);
            // Tags are already sorted

            return new Event(EventKeyFormat.encode(_consolidatedEventIds), _content, _tags);
        }

        @Override
        public int compareTo(Item item) {
            return Ints.compare(_sortIndex, item._sortIndex);
        }
    }

    // Conserve memory by having a singleton set to represent a set containing a single empty set of tags
    private final static List<List<String>> EMPTY_TAGS = ImmutableList.<List<String>>of(ImmutableList.<String>of());

    // Simple comparator for comparing lists of pre-sorted lists
    private final static Comparator<List<String>> TAG_LIST_COMPARATOR = new Comparator<List<String>>() {
        @Override
        public int compare(List<String> o1, List<String> o2) {
            int l1 = o1.size();
            int l2 = o2.size();
            int l = Math.min(l1, l2);
            for (int i=0; i < l; i++) {
                int c = o1.get(i).compareTo(o2.get(i));
                if (c != 0) {
                    return c;
                }
            }

            // Shorter list sorts first
            return Integer.compare(l1, l2);
        }
    };

    /**
     * Produces a list-of-lists for all unique tag sets for a given databus event.  <code>existingTags</code> must either
     * be null or the result of a previous call to {@link #sortedTagUnion(java.util.List, java.util.List)} or
     * {@link #sortedTagUnion(java.util.List, java.util.Set)}.
     */
    private static List<List<String>> sortedTagUnion(@Nullable List<List<String>> existingTags, Set<String> newTagSet) {
       return sortedTagUnion(existingTags, asSortedList(newTagSet));
    }

    /**
     * Produces a list-of-lists for all unique tag sets for a given databus event.  <code>existingTags</code> must either
     * be null or the result of a previous call to {@link #sortedTagUnion(java.util.List, java.util.List)} or
     * {@link #sortedTagUnion(java.util.List, java.util.Set)}.  Additionally the contents of <code>sortedNewTagList</code>
     * must already be sorted prior to this method call.
     */
    private static List<List<String>> sortedTagUnion(@Nullable List<List<String>> existingTags, List<String> sortedNewTagList) {

        // Optimize for the common case where for each coordinate there is exactly one event or that all events
        // use the same tags (typically the empty set)
        if (existingTags == null) {
            if (sortedNewTagList.isEmpty()) {
                return EMPTY_TAGS;
            }
            existingTags = Lists.newArrayListWithCapacity(3);
            existingTags.add(sortedNewTagList);
            return existingTags;
        }

        int insertionPoint = Collections.binarySearch(existingTags, sortedNewTagList, TAG_LIST_COMPARATOR);
        if (insertionPoint >= 0) {
            // Existing tags already includes this set of tags
            return existingTags;
        }

        if (existingTags == EMPTY_TAGS) {
            // Can't update the default empty tag set.  Make a copy.
            existingTags = Lists.newArrayListWithCapacity(3);
            existingTags.addAll(EMPTY_TAGS);
        }

        insertionPoint = -insertionPoint - 1;
        existingTags.add(insertionPoint, sortedNewTagList);
        return existingTags;
    }

    private static List<String> asSortedList(Set<String> tags) {
        switch (tags.size()) {
            case 0:
                return ImmutableList.of();

            case 1:
                return ImmutableList.of(tags.iterator().next());

            default:
                return Ordering.natural().immutableSortedCopy(tags);
        }

    }
}
