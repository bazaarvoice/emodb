package com.bazaarvoice.emodb.databus.db.generic;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.common.dropwizard.time.ClockTicker;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.DefaultOwnedSubscription;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import org.joda.time.Duration;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps a {@link SubscriptionDAO} with a cache that makes it fast and efficient to lookup subscription metadata.  The
 * downside is that servers must globally coordinate changes to subscriptions because the consequences of using
 * out-of-date cached subscription metadata are pretty severe.
 *
 * There have been two implementations for how cache invalidation is managed using the cache registry:
 *
 * <ul>
 *     <li>
 *         The current implementation registers a cache named "subscriptionsByName" which maps subscription names
 *         to OwnedSubscriptions.  When a subscription is changed only the corresponding subscription is invalidated.
 *     </li>
 *     <li>
 *         The legacy implementation registered a cache named "subscriptions" which mapped a single key, also named
 *         "subscriptions", to a complete map of subscription names to OwnedSubscriptions.  When a subscription was
 *         changed the single value in the cache was invalidated, causing all subscriptions to be reloaded and
 *         cached as a map.
 *     </li>
 * </ul>
 *
 * Over time, as the number of total subscriptions, single-subscription lookups and cache invalidations scaled up the
 * legacy caching became a bottleneck and was replaced with the current one.  However, since the two cache invalidation
 * systems are not compatible it is not possible to just upgrade from one to another on an in-flight system without
 * either incurring downtime or potentially losing invalidation messages.  Therefore to support in-flight upgrades
 * there is a {@link CachingMode} parameter which controls how cache invalidations are managed.
 *
 * <ol>
 *     <li>
 *         In legacy mode the DAO uses legacy cache invalidation.  It listens for invalidation messages and posts them
 *         to the legacy cache.  It also listens for current cache invalidation, although none will be posted until
 *         the next phase.
 *     </li>
 *     <li>
 *         In bridge mode the DAO still listens for legacy cache invalidations but only posts current cache invalidations.
 *     </li>
 *     <li>
 *         In normal mode the DAO exclusively uses current cache invalidation.
 *     </li>
 * </ol>
 *
 * A safe upgrade requires upgrading all servers from legacy to bridge to normal mode.  Only once all active servers
 * are in one mode is it safe to move to the next.
 */
public class CachingSubscriptionDAO implements SubscriptionDAO {

    public enum CachingMode {
        legacy,
        bridge,
        normal
    };

    private static final String SUBSCRIPTIONS = "subscriptions";

    // Loading cache cannot have null values, so use a single dummy value as a stand-in when a subscription does not exist.
    private static final OwnedSubscription NULL_SUBSCRIPTION =
            new DefaultOwnedSubscription("__null", Conditions.alwaysFalse(), new Date(0), Duration.ZERO, "__null");

    private final SubscriptionDAO _delegate;
    private final LoadingCache<String, OwnedSubscription> _subscriptionCache;
    private final LoadingCache<String, List<OwnedSubscription>> _allSubscriptionsCache;
    private final CacheHandle _subscriptionCacheHandle;
    private final CacheHandle _allSubscriptionsCacheHandle;
    private final ListeningExecutorService _refreshService;
    private final LoadingCache<String, Map<String, OwnedSubscription>> _legacyCache;
    private final CacheHandle _legacyCacheHandle;
    private final Meter _invalidationEventMeter;
    private final CachingMode _cachingMode;

    @Inject
    public CachingSubscriptionDAO(@CachingSubscriptionDAODelegate SubscriptionDAO delegate,
                                  @CachingSubscriptionDAORegistry CacheRegistry cacheRegistry,
                                  @CachingSubscriptionDAOExecutorService ListeningExecutorService refreshService,
                                  MetricRegistry metricRegistry, Clock clock, CachingMode cachingMode) {
        _delegate = checkNotNull(delegate, "delegate");
        _refreshService = checkNotNull(refreshService, "refreshService");
        _cachingMode = checkNotNull(cachingMode, "cachingMode");

        Ticker ticker = ClockTicker.getTicker(clock);

        // The all subscription cache is only used to track the set of all subscriptions and only has a single value.
        _allSubscriptionsCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .ticker(ticker)
                .recordStats()
                .build(new CacheLoader<String, List<OwnedSubscription>>() {
                    @Override
                    public List<OwnedSubscription> load(String key) throws Exception {
                        Iterable<String> subscriptionNames = _delegate.getAllSubscriptionNames();
                        ImmutableList.Builder<OwnedSubscription> subscriptions = ImmutableList.builder();

                        for (String name : subscriptionNames) {
                            // As much as possible take advantage of already cached subscriptions
                            OwnedSubscription subscription = getSubscription(name);
                            if (subscription != null) {
                                subscriptions.add(subscription);
                            }
                        }

                        return subscriptions.build();
                    }
                });

        _allSubscriptionsCacheHandle = cacheRegistry.register("allSubscriptions", _allSubscriptionsCache, true);

        _subscriptionCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .ticker(ticker)
                .removalListener((RemovalListener<String, OwnedSubscription>) notification -> {
                    // If the subscription was removed due to an explicit invalidation then to avoid a potential
                    // race condition with the corresponding _allSubscriptionsCache invalidation event proactively
                    // also invalidate the list of all subscriptions.
                    if (notification.getCause() == RemovalCause.EXPLICIT && notification.getValue() != NULL_SUBSCRIPTION) {
                        _allSubscriptionsCache.invalidate(SUBSCRIPTIONS);
                    }
                })
                .build(new CacheLoader<String, OwnedSubscription>() {
                    @Override
                    public OwnedSubscription load(String subscription) throws Exception {
                        OwnedSubscription ownedSubscription = _delegate.getSubscription(subscription);
                        if (ownedSubscription == null) {
                            // Can't cache null, use special null value
                            ownedSubscription = NULL_SUBSCRIPTION;
                        }

                        if (_cachingMode != CachingMode.normal) {
                            // Ensure there is an entry in the legacy cache so it can receive invalidation events.
                            _legacyCache.get(SUBSCRIPTIONS);
                        }

                        return ownedSubscription;
                    }

                    /**
                     * When the cached value is reloaded due to {@link CacheBuilder#refreshAfterWrite(long, TimeUnit)}
                     * having expired reload the value asynchronously.
                     */
                    @Override
                    public ListenableFuture<OwnedSubscription> reload(String key, OwnedSubscription oldValue)
                            throws Exception {
                        return _refreshService.submit(() -> load(key));
                    }
                });

        _subscriptionCacheHandle = cacheRegistry.register("subscriptionsByName", _subscriptionCache, true);

        if (cachingMode != CachingMode.normal) {
            LoggerFactory.getLogger(getClass()).info("Subscription caching mode is {}", cachingMode);

            _legacyCache = CacheBuilder.newBuilder()
                    .removalListener(notification -> {
                        // The legacy system sends a single notification when any subscription changes.  Without knowing
                        // which subscription changed the only safe action is to invalidate them all.  This is inefficient
                        // but is only necessary during an in-flight upgrade.
                        _subscriptionCache.invalidateAll();
                        _allSubscriptionsCache.invalidate(SUBSCRIPTIONS);
                    })
                    .build(new CacheLoader<String, Map<String, OwnedSubscription>>() {
                        @Override
                        public Map<String, OwnedSubscription> load(String key) throws Exception {
                            // The actual cached object doesn't matter since this cache is only used for receiving
                            // invalidation messages.  Just need to provide a non-null value.
                            return ImmutableMap.of();
                        }
                    });
            _legacyCacheHandle = cacheRegistry.register("subscriptions", _legacyCache, true);
        } else {
            _legacyCache = null;
            _legacyCacheHandle = null;
        }

        _invalidationEventMeter = metricRegistry.meter(
                MetricRegistry.name("bv.emodb.databus", "CachingSubscriptionDAO", "invalidation-events"));
    }

    @Override
    public void insertSubscription(String ownerId, String subscription, Condition tableFilter, Duration subscriptionTtl,
                                   Duration eventTtl) {
        _delegate.insertSubscription(ownerId, subscription, tableFilter, subscriptionTtl, eventTtl);

        // Invalidate this subscription.  No need to invalidate the list of all subscriptions since this will happen
        // naturally in _subscriptionCache's removal listener.
        invalidateSubscription(subscription);
    }

    @Override
    public void deleteSubscription(String subscription) {
        _delegate.deleteSubscription(subscription);

        // Synchronously tell every other server in the cluster to forget what it has cached about the subscription.
        invalidateSubscription(subscription);
    }

    private void invalidateSubscription(String subscription) {
        _invalidationEventMeter.mark();

        if (_cachingMode == CachingMode.legacy) {
            _legacyCacheHandle.invalidate(InvalidationScope.DATA_CENTER, SUBSCRIPTIONS);
        } else {
            // Invalidate the individual subscription
            _subscriptionCacheHandle.invalidate(InvalidationScope.DATA_CENTER, subscription);
            // Invalidate the list of all subscriptions.  A secondary notification is necessary because if this is
            // a new subscription then there will be no existing value in the recipient's _subscriptionCache to receive
            // an invalidation event.
            _allSubscriptionsCacheHandle.invalidate(InvalidationScope.DATA_CENTER, SUBSCRIPTIONS);
        }
    }

    @Override
    public OwnedSubscription getSubscription(String subscription) {
        // Try to load the subscription without forcing a synchronous reload.  This will return null only if it is the
        // first time the subscription has been loaded or if it has been invalidated.  The returned subscription may
        // be dirty but the cache registry will have invalidated the cached value if it were changed -- the reload is
        // only used as a failsafe.  If the value is dirty the cache will asynchronously reload it in the background.

        OwnedSubscription ownedSubscription = _subscriptionCache.getIfPresent(subscription);
        if (ownedSubscription == null) {
            // This time call get() to force the value to load, possibly synchronously.  This will also cause the value
            // to be cached.

            ownedSubscription = _subscriptionCache.getUnchecked(subscription);
        }

        // If the subscription did not exist return null and immediately remove from cache.
        if (ownedSubscription == NULL_SUBSCRIPTION) {
            _subscriptionCache.invalidate(subscription);
            ownedSubscription = null;
        }

        return ownedSubscription;
    }

    @Override
    public Iterable<OwnedSubscription> getAllSubscriptions() {
        return _allSubscriptionsCache.getUnchecked(SUBSCRIPTIONS);
    }

    @Override
    public Iterable<String> getAllSubscriptionNames() {
        return Iterables.transform(getAllSubscriptions(), OwnedSubscription::getName);
    }
}
