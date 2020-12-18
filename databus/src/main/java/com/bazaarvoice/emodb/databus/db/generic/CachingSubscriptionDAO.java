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
import com.codahale.metrics.Timer;
import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.ForwardingLoadingCache;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps a {@link SubscriptionDAO} with a cache that makes it fast and efficient to lookup subscription metadata.  The
 * downside is that servers must globally coordinate changes to subscriptions because the consequences of using
 * out-of-date cached subscription metadata are pretty severe.
 * <p>
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
 * <p>
 * Over time, as the number of total subscriptions, single-subscription lookups and cache invalidations scaled up the
 * legacy caching became a bottleneck and was replaced with the current one.  However, since the two cache invalidation
 * systems are not compatible it is not possible to just upgrade from one to another on an in-flight system without
 * either incurring downtime or potentially losing invalidation messages.  Therefore to support in-flight upgrades
 * there is a {@link CachingMode} parameter which controls how cache invalidations are managed.
 *
 * <ol>
 *     <li>
 *         In legacy mode the DAO listens for invalidation messages and posts them to both the legacy cache and
 *         current cache.
 *     </li>
 *     <li>
 *         In normal mode the DAO exclusively uses current cache invalidation.
 *     </li>
 * </ol>
 * <p>
 * A safe upgrade requires upgrading all servers from legacy to normal mode.  Only once all active servers are in one
 * mode is it safe to move to the next.
 */
public class CachingSubscriptionDAO implements SubscriptionDAO {

    public enum CachingMode {
        legacy,
        normal
    }

    private static final String SUBSCRIPTIONS = "subscriptions";

    // Loading cache cannot have null values, so use a single dummy value as a stand-in when a subscription does not exist.
    private static final OwnedSubscription NULL_SUBSCRIPTION =
            new DefaultOwnedSubscription("__null", Conditions.alwaysFalse(), new Date(0), Duration.ZERO, "__null");

    private final SubscriptionDAO _delegate;
    private final LoadingCache<String, OwnedSubscription> _subscriptionCache;
    private final LoadingCache<String, List<OwnedSubscription>> _allSubscriptionsCache;
    private final CacheHandle _subscriptionCacheHandle;
    private final ListeningExecutorService _refreshService;
    private final LoadingCache<String, Map<String, OwnedSubscription>> _legacyCache;
    private final CacheHandle _legacyCacheHandle;
    private final Meter _invalidationEventMeter;
    private final Meter _subscriptionFromCacheMeter;
    private final Meter _subscriptionFromDelegateMeter;
    private final Timer _reloadAllSubscriptionsTimer;
    private final Meter _subscriptionsReloaded;
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
        _reloadAllSubscriptionsTimer = metricRegistry.timer(MetricRegistry.name("bv.emodb.databus", "CachingSubscriptionDAO", "reload-all-subscriptions"));
        _subscriptionsReloaded = metricRegistry.meter(MetricRegistry.name("bv.emodb.databus", "CachingSubscriptionDAO", "subscriptions-reloaded"));

        // The all subscription cache is only used to track the set of all subscriptions and only has a single value.
        _allSubscriptionsCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .ticker(ticker)
                .recordStats()
                .build(new CacheLoader<String, List<OwnedSubscription>>() {
                    @Override
                    public List<OwnedSubscription> load(String key) throws Exception {
                        try (Timer.Context ignore = _reloadAllSubscriptionsTimer.time()) {
                            assert SUBSCRIPTIONS.equals(key) : "All subscriptions cache should only be accessed by a single key";

                            Iterable<String> subscriptionNames = _delegate.getAllSubscriptionNames();
                            ImmutableList.Builder<OwnedSubscription> subscriptionsBuilder = ImmutableList.builder();

                            for (String name : subscriptionNames) {
                                // As much as possible take advantage of already cached subscriptions
                                OwnedSubscription subscription = getSubscription(name);
                                if (subscription != null) {
                                    subscriptionsBuilder.add(subscription);
                                }
                            }

                            List<OwnedSubscription> subscriptions = subscriptionsBuilder.build();
                            _subscriptionsReloaded.mark(subscriptions.size());
                            return subscriptions;
                        }
                    }
                });

        final LoadingCache<String, OwnedSubscription> rawSubscriptionCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .ticker(ticker)
                .recordStats()
                .build(new CacheLoader<String, OwnedSubscription>() {
                    @Override
                    public OwnedSubscription load(String subscription) throws Exception {
                        OwnedSubscription ownedSubscription = _delegate.getSubscription(subscription);
                        // Can't cache null, use special null value if the subscription does not exist
                        return Optional.ofNullable(ownedSubscription).orElse(NULL_SUBSCRIPTION);
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

        // Add a forwarding layer to the raw subscription cache to handle several custom responses.
        _subscriptionCache = new InvalidationListeningForwardingCache<String, OwnedSubscription>(rawSubscriptionCache) {

            @Override
            protected void valueInvalidated() {
                // If any subscription is invalidated also invalidate the full list of subscriptions to force a refresh.
                _allSubscriptionsCache.invalidate(SUBSCRIPTIONS);
            }

            // The only get methods called by the DAO are getIfPresent() and getUnchecked().  Override both
            // to invalidate non-existent subscriptions immediately so we don't leak memory if there is a flood of
            // requests for non-existent subscriptions.

            @Nullable
            @Override
            public OwnedSubscription getIfPresent(Object key) {
                return getWithNullInvalidation(key, super.getIfPresent(key));
            }

            @Override
            public OwnedSubscription getUnchecked(String key) {
                return getWithNullInvalidation(key, super.getUnchecked(key));
            }

            private OwnedSubscription getWithNullInvalidation(Object key, @Nullable OwnedSubscription subscription) {
                if (subscription == NULL_SUBSCRIPTION) {
                    delegate().invalidate(key);
                }
                return subscription;
            }
        };

        _subscriptionCacheHandle = cacheRegistry.register("subscriptionsByName", _subscriptionCache, true);

        if (cachingMode == CachingMode.legacy) {
            LoggerFactory.getLogger(getClass()).info("Subscription caching mode is {}", cachingMode);

            _legacyCache = new InvalidationListeningForwardingCache<String, Map<String, OwnedSubscription>>(
                    CacheBuilder.newBuilder().build(new CacheLoader<String, Map<String, OwnedSubscription>>() {
                        @Override
                        public Map<String, OwnedSubscription> load(String key) throws Exception {
                            // The actual cached object doesn't matter since this cache is only used for receiving
                            // invalidation messages.  Just need to provide a non-null value.
                            return ImmutableMap.of();
                        }
                    })
            ) {
                @Override
                protected void valueInvalidated() {
                    // The legacy system sends a single notification when any subscription changes.  Without knowing
                    // which subscription changed the only safe action is to invalidate them all.  This is inefficient
                    // but is only necessary during an in-flight upgrade.
                    _subscriptionCache.invalidateAll();
                }
            };

            _legacyCacheHandle = cacheRegistry.register("subscriptions", _legacyCache, true);
        } else {
            _legacyCache = null;
            _legacyCacheHandle = null;
        }

        _invalidationEventMeter = metricRegistry.meter(
                MetricRegistry.name("bv.emodb.databus", "CachingSubscriptionDAO", "invalidation-events"));
        _subscriptionFromCacheMeter = metricRegistry.meter(
                MetricRegistry.name("bv.emodb.databus", "CachingSubscriptionDAO", "get-subscription-from-cache"));
        _subscriptionFromDelegateMeter = metricRegistry.meter(
                MetricRegistry.name("bv.emodb.databus", "CachingSubscriptionDAO", "get-subscription-from-delegate"));
    }

    @Override
    public void insertSubscription(String ownerId, String subscription, Condition tableFilter, Duration subscriptionTtl,
                                   Duration eventTtl) {
        _delegate.insertSubscription(ownerId, subscription, tableFilter, subscriptionTtl, eventTtl);

        // Invalidate this subscription.  No need to invalidate the list of all subscriptions since this will happen
        // naturally in _subscriptionCache's invalidation handler.
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

        _subscriptionCacheHandle.invalidate(InvalidationScope.DATA_CENTER, subscription);

        // If in legacy mode also invalidate the legacy cache
        if (_cachingMode == CachingMode.legacy) {
            _legacyCacheHandle.invalidate(InvalidationScope.DATA_CENTER, SUBSCRIPTIONS);
        }
    }

    @Override
    public OwnedSubscription getSubscription(String subscription) {
        // Try to load the subscription without forcing a synchronous reload.  This will return null only if it is the
        // first time the subscription has been loaded or if it has been invalidated.  The returned subscription may
        // be dirty but the cache registry will have invalidated the cached value if it were changed -- the reload is
        // only used as a failsafe.  If the value is dirty the cache will asynchronously reload it in the background.

        OwnedSubscription ownedSubscription = _subscriptionCache.getIfPresent(subscription);
        if (ownedSubscription != null) {
            _subscriptionFromCacheMeter.mark();
        } else {
            // This time call get() to force the value to load, possibly synchronously.  This will also cause the value
            // to be cached.

            ownedSubscription = _subscriptionCache.getUnchecked(subscription);
            _subscriptionFromDelegateMeter.mark();
        }

        // If the subscription did not exist return null
        if (ownedSubscription == NULL_SUBSCRIPTION) {
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

    /**
     * Custom forwarding cache used by this class.  Any time a subscription is invalidated the single value in
     * {@link #_allSubscriptionsCache} also needs to be invalidated.  Normally this could be accomplished by providing
     * a listener to {@link CacheBuilder#removalListener(RemovalListener)}.  However, that listener only receives
     * notification if a cached value is actually removed.  If a brand new subscription is created remotely then
     * there will be no locally cached entry and therefore no notification.  Therefore, the solution instead is to
     * wrap the cache with a forwarding implementation which calls {@link InvalidationListeningForwardingCache#valueInvalidated()}
     * whenever <em>any</em> value is removed since the action taken on any value invalidation is the same.
     */
    private abstract static class InvalidationListeningForwardingCache<K, V> extends ForwardingLoadingCache.SimpleForwardingLoadingCache<K, V> {

        InvalidationListeningForwardingCache(LoadingCache<K, V> delegate) {
            super(delegate);
        }

        protected abstract void valueInvalidated();

        @Override
        public void invalidate(Object key) {
            super.invalidate(key);
            valueInvalidated();
        }

        @Override
        public void invalidateAll(Iterable<?> keys) {
            super.invalidateAll(keys);
            valueInvalidated();
        }

        @Override
        public void invalidateAll() {
            super.invalidateAll();
            valueInvalidated();
        }
    }
}
