package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.common.dropwizard.time.ClockTicker;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.DefaultJoinFilter;
import com.bazaarvoice.emodb.databus.MasterFanoutPartitions;
import com.bazaarvoice.emodb.databus.QueueDrainExecutorService;
import com.bazaarvoice.emodb.databus.SystemIdentity;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Names;
import com.bazaarvoice.emodb.databus.api.PollResult;
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
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DataProvider;
import com.bazaarvoice.emodb.sor.core.DatabusEventWriter;
import com.bazaarvoice.emodb.sor.core.DatabusEventWriterRegistry;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.bazaarvoice.emodb.sortedq.core.ReadOnlyQueueException;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DefaultDatabus implements OwnerAwareDatabus, DatabusEventWriter, Managed {

    private static final Logger _log = LoggerFactory.getLogger(DefaultDatabus.class);

    /**
     * How long should poll loop, searching for events before giving up and returning.
     */
    private static final Duration MAX_POLL_TIME = Duration.ofMillis(100);

    /**
     * How long should the app wait before querying the data store again when it finds an unknown change?
     */
    private static final Duration RECENT_UNKNOWN_RETRY = Duration.ofMillis(400);

    /**
     * How old does an event need to be before we stop aggressively looking for it?
     */
    private static final Duration STALE_UNKNOWN_AGE = Duration.ofSeconds(2);

    /**
     * Don't merge too many duplicate events together to avoid event keys getting unreasonably long.
     */
    private static final int MAX_EVENTS_TO_CONSOLIDATE = 1000;

    /* How many items are to be fetched in each try for draining the queue. */
    private static final int MAX_ITEMS_TO_FETCH_FOR_QUEUE_DRAINING = 100;

    /* This is how long we submit tasks to drain the queue for each subscription from one poll request */
    private static final Duration MAX_QUEUE_DRAIN_TIME_FOR_A_SUBSCRIPTION = Duration.ofMinutes(1);

    /* We don't allow subscriptions to be created beyond this number. */
    private static final Duration MAX_SUBSCRIPTION_TTL = Duration.ofDays(365 * 10);
    /* We don't allow subscriptions to be created with event TTLs beyond this number. */
    public static final Duration MAX_EVENT_TTL = Duration.ofDays(365);

    private final DatabusEventWriterRegistry _eventWriterRegistry;
    private final SubscriptionDAO _subscriptionDao;
    private final DatabusEventStore _eventStore;
    private final DataProvider _dataProvider;
    private final SubscriptionEvaluator _subscriptionEvaluator;
    private final JobService _jobService;
    private final DatabusAuthorizer _databusAuthorizer;
    private final String _systemOwnerId;
    private final PartitionSelector _masterPartitionSelector;
    private final List<String> _masterFanoutChannels;
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
    private final Meter _drainQueueAsyncMeter;
    private final Meter _drainQueueTaskMeter;
    private final Meter _drainQueueRedundantMeter;
    private final LoadingCache<SizeCacheKey, Map.Entry<Long, Long>> _eventSizeCache;
    private final Supplier<Condition> _defaultJoinFilterCondition;
    private final Ticker _ticker;
    private final Clock _clock;
    private ExecutorService _drainService;
    private ConcurrentMap<String, Long> _drainedSubscriptionsMap = Maps.newConcurrentMap();

    @Inject
    public DefaultDatabus(LifeCycleRegistry lifeCycle, DatabusEventWriterRegistry eventWriterRegistry,
                          DataProvider dataProvider, SubscriptionDAO subscriptionDao, DatabusEventStore eventStore,
                          SubscriptionEvaluator subscriptionEvaluator, JobService jobService,
                          JobHandlerRegistry jobHandlerRegistry, DatabusAuthorizer databusAuthorizer,
                          @SystemIdentity String systemOwnerId,
                          @DefaultJoinFilter Supplier<Condition> defaultJoinFilterCondition,
                          @QueueDrainExecutorService ExecutorService drainService,
                          @MasterFanoutPartitions int masterPartitions,
                          @MasterFanoutPartitions PartitionSelector masterPartitionSelector,
                          MetricRegistry metricRegistry, Clock clock) {
        _eventWriterRegistry = eventWriterRegistry;
        _subscriptionDao = subscriptionDao;
        _eventStore = eventStore;
        _dataProvider = dataProvider;
        _subscriptionEvaluator = subscriptionEvaluator;
        _jobService = jobService;
        _databusAuthorizer = databusAuthorizer;
        _systemOwnerId = systemOwnerId;
        _defaultJoinFilterCondition = defaultJoinFilterCondition;
        _drainService = checkNotNull(drainService, "drainService");
        _masterPartitionSelector = masterPartitionSelector;
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
        _drainQueueAsyncMeter = newEventMeter("drainQueueAsync", metricRegistry);
        _drainQueueTaskMeter = newEventMeter("drainQueueTask", metricRegistry);
        _drainQueueRedundantMeter = newEventMeter("drainQueueRedundant", metricRegistry);
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

        ImmutableList.Builder<String> masterFanoutChannels = ImmutableList.builder();
        for (int partition=0; partition < masterPartitions; partition++) {
            masterFanoutChannels.add(ChannelNames.getMasterFanoutChannel(partition));
        }
        _masterFanoutChannels = masterFanoutChannels.build();
        
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
                                if (request.getSince() != null && request.getSince().toInstant()
                                        .plus(DatabusChannelConfiguration.REPLAY_TTL)
                                        .isBefore(_clock.instant())) {
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
                Duration.ofDays(3650), DatabusChannelConfiguration.REPLAY_TTL, false);
    }

    private Meter newEventMeter(String name, MetricRegistry metricRegistry) {
        return metricRegistry.meter(getMetricName(name));
    }

    private String getMetricName(String name) {
        return MetricRegistry.name("bv.emodb.databus", "DefaultDatabus", name);
    }

    @VisibleForTesting
    protected ConcurrentMap<String, Long> getDrainedSubscriptionsMap() {
        return _drainedSubscriptionsMap;
    }

    @Override
    public void start()
            throws Exception {
        // Create a databus replay subscription
        createDatabusReplaySubscription();
        _eventWriterRegistry.registerDatabusEventWriter(this);
    }

    @Override
    public void stop()
            throws Exception {
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
        checkArgument(subscriptionTtl.compareTo(Duration.ZERO) > 0, "SubscriptionTtl must be >0");
        checkArgument(subscriptionTtl.compareTo(MAX_SUBSCRIPTION_TTL) <= 0, "Subscription TTL duration limit is 10 years. The value cannot go beyond that.");
        checkArgument(eventTtl.compareTo(Duration.ZERO) > 0, "EventTtl must be >0");
        checkArgument(eventTtl.compareTo(MAX_EVENT_TTL) <= 0, "Event TTL duration limit is 365 days. The value cannot go beyond that.");
        SubscriptionConditionValidator.checkAllowed(tableFilter);

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
    public Subscription getSubscription(String ownerId, String name)
            throws UnknownSubscriptionException {
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

    @Override
    public void writeEvents(Collection<UpdateRef> refs) {
        ImmutableMultimap.Builder<String, ByteBuffer> eventIds = ImmutableMultimap.builder();
        for (UpdateRef ref : refs) {
            int partition = _masterPartitionSelector.getPartition(ref.getKey());
            eventIds.put(_masterFanoutChannels.get(partition), UpdateRefSerializer.toByteBuffer(ref));
        }
        _eventStore.addAll(eventIds.build());
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
    public Iterator<Event> peek(String ownerId, final String subscription, int limit) {
        checkLegalSubscriptionName(subscription);
        checkArgument(limit > 0, "Limit must be >0");
        checkSubscriptionOwner(ownerId, subscription);

        PollResult result = peekOrPoll(subscription, null, limit);
        return result.getEventIterator();
    }

    @Override
    public PollResult poll(String ownerId, final String subscription, final Duration claimTtl, int limit) {
        checkLegalSubscriptionName(subscription);
        checkArgument(claimTtl.compareTo(Duration.ZERO) >= 0, "ClaimTtl must be >=0");
        checkArgument(limit > 0, "Limit must be >0");
        checkSubscriptionOwner(ownerId, subscription);

        return peekOrPoll(subscription, claimTtl, limit);
    }

    /** Implements peek() or poll() based on whether claimTtl is null or non-null. */
    private PollResult peekOrPoll(String subscription, @Nullable Duration claimTtl, int limit) {
        int remaining = limit;
        Map<Coordinate, EventList> rawEvents = ImmutableMap.of();
        Map<Coordinate, Item> uniqueItems = Maps.newHashMap();
        boolean isPeek = claimTtl == null;
        boolean repeatable = !isPeek && claimTtl.toMillis() > 0;
        boolean eventsAvailableForNextPoll = false;
        boolean noMaxPollTimeOut = true;
        int itemsDiscarded = 0;
        Meter eventMeter = isPeek ? _peekedMeter : _polledMeter;

        // Reading raw events from the event store is a significantly faster operation than resolving the events into
        // databus poll events.  This is because the former sequentially loads small event references while the latter
        // requires reading and resolving the effectively random objects associated with those references from the data
        // store.
        //
        // To make the process more efficient this method first polls for "limit" raw events from the event store.
        // Then, up to the first 10 of those raw events are resolved synchronously with a time limit of MAX_POLL_TIME.
        // Any remaining raw events are resolved lazily as the event list is consumed by the caller.  This makes the
        // return time for this method faster and more predictable while supporting polls for more events than can
        // be resolved within MAX_POLL_TIME.  This is especially beneficial for REST clients which may otherwise time
        // out while waiting for "limit" events to be read and resolved.

        Stopwatch stopwatch = Stopwatch.createStarted(_ticker);
        int padding = 0;
        do {
            if (remaining == 0) {
                break;  // Don't need any more events.
            }

            // Query the databus event store.  Consolidate multiple events that refer to the same item.
            ConsolidatingEventSink sink = new ConsolidatingEventSink(remaining + padding);
            boolean more = isPeek ?
                    _eventStore.peek(subscription, sink) :
                    _eventStore.poll(subscription, claimTtl, sink);
            rawEvents = sink.getEvents();

            if (rawEvents.isEmpty()) {
                // No events to be had.
                eventsAvailableForNextPoll = more;
                break;
            }

            // Resolve the raw events in batches of 10 until at least one response item is found for a maximum time of MAX_POLL_TIME.
            do {
                int batchItemsDiscarded = resolvePeekOrPollEvents(subscription, rawEvents, Math.min(10, remaining),
                        (coord, item) -> {
                            // Check whether we've already added this piece of content to the poll result.  If so, consolidate
                            // the two together to reduce the amount of work a client must do.  Note that the previous item
                            // would be from a previous batch of events and it's possible that we have read two different
                            // versions of the same item of content.  This will prefer the most recent.
                            Item previousItem = uniqueItems.get(coord);
                            if (previousItem != null && previousItem.consolidateWith(item)) {
                                _consolidatedMeter.mark();
                            } else {
                                // We have found a new item of content to return!
                                uniqueItems.put(coord, item);
                            }
                        });
                remaining = limit - uniqueItems.size();
                itemsDiscarded += batchItemsDiscarded;
            } while (!rawEvents.isEmpty() && remaining > 0 && stopwatch.elapsed(TimeUnit.MILLISECONDS) < MAX_POLL_TIME.toMillis());

            // There are more events for the next poll if either the event store explicitly said so or if, due to padding,
            // we got more events than "limit", in which case we're likely to unclaim at last one.
            eventsAvailableForNextPoll = more || rawEvents.size() + uniqueItems.size() > limit;
            if (!more) {
                // There are no more events to be had, so exit now
                break;
            }

            // Note: Due to redundant/unknown events, it's possible that the 'events' list is empty even though, if we
            // tried again, we'd find more events.  Try again a few times, but not for more than MAX_POLL_TIME so clients
            // don't timeout the request.  This helps move through large amounts of redundant deltas relatively quickly
            // while also putting a bound on the total amount of work done by a single call to poll().
            padding = 10;
        } while (repeatable && (noMaxPollTimeOut = stopwatch.elapsed(TimeUnit.MILLISECONDS) < MAX_POLL_TIME.toMillis()));

        Iterator<Event> events;
        int approximateSize;
        if (uniqueItems.isEmpty()) {
            // Either there were no raw events or all events found were for redundant or unknown changes.  It's possible
            // that eventually there will be more events, but to prevent a lengthy delay iterating the remaining events
            // quit now and return an empty result.  The caller can always poll again to try to pick up any more events,
            // and if necessary an async drain will kick off a few lines down to assist in clearing the redundant update
            // wasteland.
            events = Iterators.emptyIterator();
            approximateSize = 0;

            // If there are still more unresolved events claimed then unclaim them now
            if (repeatable && !rawEvents.isEmpty()) {
                unclaim(subscription, rawEvents.values());
            }
        } else if (rawEvents.isEmpty()) {
            // All events have been resolved
            events = toEvents(uniqueItems.values()).iterator();
            approximateSize = uniqueItems.size();
            eventMeter.mark(approximateSize);
        } else {
            // Return an event list which contains the first events which were resolved synchronously plus the
            // remaining events from the peek or poll which will be resolved lazily in batches of 25.

            final Map<Coordinate, EventList> deferredRawEvents = Maps.newLinkedHashMap(rawEvents);
            final int initialDeferredLimit = remaining;

            Iterator<Event> deferredEvents = new AbstractIterator<Event>() {
                private Iterator<Event> currentBatch = Iterators.emptyIterator();
                private int remaining = initialDeferredLimit;

                @Override
                protected Event computeNext() {
                    Event next = null;

                    if (currentBatch.hasNext()) {
                        next = currentBatch.next();
                    } else if (!deferredRawEvents.isEmpty() && remaining > 0) {
                        // Resolve the next batch of events
                        try {
                            final List<Item> items = Lists.newArrayList();
                            do {
                                resolvePeekOrPollEvents(subscription, deferredRawEvents, Math.min(remaining, 25),
                                        (coord, item) -> {
                                            // Unlike with the original batch the deferred batch's events are always
                                            // already de-duplicated by coordinate, so there is no need to maintain
                                            // a coordinate-to-item uniqueness map.
                                            items.add(item);
                                        });

                                if (!items.isEmpty()) {
                                    remaining -= items.size();
                                    currentBatch = toEvents(items).iterator();
                                    next = currentBatch.next();
                                }
                            } while (next == null && !deferredRawEvents.isEmpty() && remaining > 0);
                        } catch (Exception e) {
                            // Don't fail; the caller has already received some events.  Just cut the result stream short
                            // now and throw back any remaining events for a future poll.
                            _log.warn("Failed to load additional events during peek/poll for subscription {}", subscription, e);
                        }
                    }

                    if (next != null) {
                        return next;
                    }

                    if (!deferredRawEvents.isEmpty()) {
                        // If we padded the number of raw events it's possible there are more than the caller actually
                        // requested.  Release the extra padded events now.
                        try {
                            unclaim(subscription, deferredRawEvents.values());
                        } catch (Exception e) {
                            // Don't fail, just log a warning.  The claims will eventually time out on their own.
                            _log.warn("Failed to unclaim {} events from subscription {}", deferredRawEvents.size(), subscription, e);
                        }
                    }

                    // Update the metric for the actual number of events returned
                    eventMeter.mark(limit - remaining);

                    return endOfData();
                }
            };

            events = Iterators.concat(toEvents(uniqueItems.values()).iterator(), deferredEvents);
            approximateSize = uniqueItems.size() + deferredRawEvents.size();
        }

        // Try draining the queue asynchronously if there are still more events available and more redundant events were
        // discarded than resolved items found so far.
        // Doing this only in the poll case for now.
        if (repeatable && eventsAvailableForNextPoll && itemsDiscarded > uniqueItems.size()) {
            drainQueueAsync(subscription);
        }

        return new PollResult(events, approximateSize, eventsAvailableForNextPoll);
    }

    /**
     * Resolves events found during a peek or poll and converts them into items.  No more than <code>limit</code>
     * events are read, not including events which are skipped because they come from dropped tables.
     *
     * Any events for content which has not yet been replicated to the local data center are excluded and set to retry
     * in RECENT_UNKNOWN_RETRY.  Any events for redundant changes are automatically deleted.
     *
     * To make use of this method more efficient it is not idempotent.  This method has the following side effect:
     *
     * <ol>
     *     <li>All events processed are removed from <code>rawEvents</code>.</li>
     * </ol>
     *
     * Finally, this method returns the number of redundant events that were found and deleted, false otherwise.
     */
    private int resolvePeekOrPollEvents(String subscription, Map<Coordinate, EventList> rawEvents, int limit,
                                            ResolvedItemSink sink) {
        Map<Coordinate, Integer> eventOrder = Maps.newHashMap();
        List<String> eventIdsToDiscard = Lists.newArrayList();
        List<String> recentUnknownEventIds = Lists.newArrayList();
        int remaining = limit;
        int itemsDiscarded = 0;

        DataProvider.AnnotatedGet annotatedGet = _dataProvider.prepareGetAnnotated(ReadConsistency.STRONG);
        Iterator<Map.Entry<Coordinate, EventList>> rawEventIterator = rawEvents.entrySet().iterator();

        while (rawEventIterator.hasNext() && remaining != 0) {
            Map.Entry<Coordinate, EventList> entry = rawEventIterator.next();
            Coordinate coord = entry.getKey();

            // Query the table/key pair.
            try {
                annotatedGet.add(coord.getTable(), coord.getId());
                remaining -= 1;
            } catch (UnknownTableException | UnknownPlacementException e) {
                // It's likely the table or facade was dropped since the event was queued.  Discard the events.
                EventList list = entry.getValue();
                for (Pair<String, UUID> pair : list.getEventAndChangeIds()) {
                    eventIdsToDiscard.add(pair.first());
                }
                _discardedMeter.mark(list.size());
            }

            // Keep track of the order in which we received the events from the EventStore.
            eventOrder.put(coord, eventOrder.size());
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

            Item item = null;

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

                Item eventItem = new Item(eventId, eventOrder.get(coord), content, tags);
                if (item == null) {
                    item = eventItem;
                } else if (item.consolidateWith(eventItem)) {
                    _consolidatedMeter.mark();
                } else {
                    sink.accept(coord, item);
                    item = eventItem;
                }
            }

            if (item != null) {
                sink.accept(coord, item);
            }
        }

        // Reduce the claim length on recent unknown IDs so we look for them again soon.
        if (!recentUnknownEventIds.isEmpty()) {
            _eventStore.renew(subscription, recentUnknownEventIds, RECENT_UNKNOWN_RETRY, false);
        }
        // Ack events we never again want to see.
        if ((itemsDiscarded = eventIdsToDiscard.size()) != 0) {
            _eventStore.delete(subscription, eventIdsToDiscard, true);
        }
        // Remove all coordinates from rawEvents which were processed by this method
        for (Coordinate coord : eventOrder.keySet()) {
            rawEvents.remove(coord);
        }

        return itemsDiscarded;
    }

    /**
     * Simple interface for the event sink in {@link #resolvePeekOrPollEvents(String, Map, int, ResolvedItemSink)}
     */
    private interface ResolvedItemSink {
        void accept(Coordinate coordinate, Item item);
    }

    /**
     * Converts a collection of Items to Events.
     */
    private List<Event> toEvents(Collection<Item> items) {
        if (items.isEmpty()) {
            return ImmutableList.of();
        }

        // Sort the items to match the order of their events in an attempt to get first-in-first-out.
        return items.stream().sorted().map(Item::toEvent).collect(Collectors.toList());
    }

    /**
     * Convenience method to unclaim all of the events from a collection of event lists.  This is to unclaim excess
     * events when a padded poll returns more events than the requested limit.
     */
    private void unclaim(String subscription, Collection<EventList> eventLists) {
        List<String> eventIdsToUnclaim = Lists.newArrayList();
        for (EventList unclaimEvents : eventLists) {
            for (Pair<String, UUID> eventAndChangeId : unclaimEvents.getEventAndChangeIds()) {
                eventIdsToUnclaim.add(eventAndChangeId.first());
            }
        }
        _eventStore.renew(subscription, eventIdsToUnclaim, Duration.ZERO, false);

    }

    private boolean isRecent(UUID changeId) {
        return _clock.millis() - TimeUUIDs.getTimeMillis(changeId) < STALE_UNKNOWN_AGE.toMillis();
    }

    @Override
    public void renew(String ownerId, String subscription, Collection<String> eventKeys, Duration claimTtl) {
        checkLegalSubscriptionName(subscription);
        checkNotNull(eventKeys, "eventKeys");
        checkArgument(claimTtl.compareTo(Duration.ZERO) >= 0, "ClaimTtl must be >=0");
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
        checkState(since == null || since.toInstant().plus(DatabusChannelConfiguration.REPLAY_TTL).isAfter(_clock.instant()),
                "Since timestamp is outside the replay TTL.");
        String source = ChannelNames.getMasterReplayChannel();
        final OwnedSubscription destination = getSubscriptionByName(subscription);

        _eventStore.copy(source, subscription,
                (eventDataBytes) -> _subscriptionEvaluator.matches(destination, eventDataBytes, since),
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

    @VisibleForTesting
    protected void drainQueueAsync(String subscription) {
        try {
            boolean notDrainingNow = _drainedSubscriptionsMap.putIfAbsent(subscription, 0L) == null;
            if (notDrainingNow) {
                _log.info("Starting the draining process for subscription: {}.", subscription);
                _drainQueueAsyncMeter.mark();
                // submit a task to drain the queue.
                try {
                    submitDrainServiceTask(subscription, MAX_ITEMS_TO_FETCH_FOR_QUEUE_DRAINING, null);
                } catch (Exception e) {
                    // Failed to submit the task.  This is unlikely, but just in case clear the draining marker
                    _drainedSubscriptionsMap.remove(subscription);
                }
            } else {
                _log.debug("Draining for subscription: {} was already started from a previous poll.", subscription);
            }
        } catch (Exception e) {
            _log.error("Encountered exception while draining the queue for subscription: {}.", subscription, e);
        }
    }

    private void submitDrainServiceTask(String subscription, int itemsToFetch, @Nullable Cache<String, Boolean> knownNonRedundantEvents) {
        _drainQueueTaskMeter.mark();
        _drainService.submit(new Runnable() {
            @Override
            public void run() {
                doDrainQueue(subscription, itemsToFetch,
                        knownNonRedundantEvents != null ? knownNonRedundantEvents : CacheBuilder.newBuilder().maximumSize(1000).build());
            }
        });
    }

    private void doDrainQueue(String subscription, int itemsToFetch, Cache<String, Boolean> knownNonRedundantEvents) {
        boolean anyRedundantItemFound = false;
        Stopwatch stopwatch = Stopwatch.createStarted(_ticker);

        ConsolidatingEventSink sink = new ConsolidatingEventSink(itemsToFetch);
        boolean more = _eventStore.peek(subscription, sink);

        Map<Coordinate, EventList> rawEvents = sink.getEvents();

        if (rawEvents.isEmpty()) {
            _drainedSubscriptionsMap.remove(subscription);
            return;  // queue is empty.
        }

        List<String> eventIdsToDiscard = Lists.newArrayList();

        // Query the events from the data store in batch to reduce latency.
        DataProvider.AnnotatedGet annotatedGet = _dataProvider.prepareGetAnnotated(ReadConsistency.STRONG);
        for (Map.Entry<Coordinate, EventList> entry : rawEvents.entrySet()) {
            Coordinate coord = entry.getKey();

            // If we've determined on a previous iteration that all change IDs returned for this coordinate are
            // not redundant then skip it.

            boolean anyUnverifiedEvents = entry.getValue().getEventAndChangeIds().stream()
                    .map(Pair::first)
                    .anyMatch(eventId -> knownNonRedundantEvents.getIfPresent(eventId) == null);

            if (anyUnverifiedEvents) {
                // Query the table/key pair.
                try {
                    annotatedGet.add(coord.getTable(), coord.getId());
                } catch (UnknownTableException | UnknownPlacementException e) {
                    // It's likely the table or facade was dropped since the event was queued.  Discard the events.
                    EventList list = entry.getValue();
                    for (Pair<String, UUID> pair : list.getEventAndChangeIds()) {
                        eventIdsToDiscard.add(pair.first());
                    }
                }
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

            // Loop over the Databus events for this piece of content.  Usually there's just one, but not always...
            for (Pair<String, UUID> eventData : eventList.getEventAndChangeIds()) {
                String eventId = eventData.first();
                UUID changeId = eventData.second();

                // Is the change redundant?
                if (readResult.isChangeDeltaRedundant(changeId)) {
                    anyRedundantItemFound = true;
                    eventIdsToDiscard.add(eventId);
                } else {
                    knownNonRedundantEvents.put(eventId, Boolean.TRUE);
                }
            }
        }

        // delete the events we never again want to see.
        if (!eventIdsToDiscard.isEmpty()) {
            _drainQueueRedundantMeter.mark(eventIdsToDiscard.size());
            _eventStore.delete(subscription, eventIdsToDiscard, true);
        }

        long totalSubscriptionDrainTime = _drainedSubscriptionsMap.getOrDefault(subscription, 0L) + stopwatch.elapsed(TimeUnit.MILLISECONDS);

        // submit a new task for next batch if any the found items in this batch are redundant and there are more items available on the queue.
        // Also right now, we are only giving MAX_QUEUE_DRAIN_TIME_FOR_A_SUBSCRIPTION time for draining for a subscription for each poll. This is because there is no guarantee that the local server
        // remains as the owner of the subscription through out. The right solution here is to check if the local service still owns the subscription for each task submission.
        // But, MAX_QUEUE_DRAIN_TIME_FOR_A_SUBSCRIPTION may be OK as we can easily expect subsequent polls from the clients which will trigger these tasks again.
        if (anyRedundantItemFound && more && totalSubscriptionDrainTime < MAX_QUEUE_DRAIN_TIME_FOR_A_SUBSCRIPTION.toMillis()) {
            _drainedSubscriptionsMap.replace(subscription, totalSubscriptionDrainTime);
            submitDrainServiceTask(subscription, itemsToFetch, knownNonRedundantEvents);
        } else {
            _drainedSubscriptionsMap.remove(subscription);
        }
    }

    /**
     * EventStore sink that doesn't count adjacent events for the same table/key against the peek/poll limit.
     */
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

        boolean consolidateWith(Item other) {
            if (_consolidatedEventIds.size() >= MAX_EVENTS_TO_CONSOLIDATE) {
                return false;
            }

            // We'll construct an event key that combines all the duplicate events.  Unfortunately we can't discard/ack
            // the extra events at this time, subtle race conditions result that can cause clients to miss events.
            _consolidatedEventIds.addAll(other._consolidatedEventIds);

            // Pick the newest version of the content.  There's no reason to return stale stuff.
            if (Intrinsic.getVersion(_content) < Intrinsic.getVersion(other._content)) {
                _content = other._content;
            }

            // Combine tags from the other event
            for (List<String> tagList : other._tags) {
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
            for (int i = 0; i < l; i++) {
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
