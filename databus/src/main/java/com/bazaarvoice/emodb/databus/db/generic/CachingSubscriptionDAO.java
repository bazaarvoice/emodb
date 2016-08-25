package com.bazaarvoice.emodb.databus.db.generic;

import com.bazaarvoice.emodb.cachemgr.api.CacheHandle;
import com.bazaarvoice.emodb.cachemgr.api.CacheRegistry;
import com.bazaarvoice.emodb.cachemgr.api.InvalidationScope;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.joda.time.Duration;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Wraps a {@link SubscriptionDAO} with a cache that makes it fast and efficient to lookup subscription metadata.  The
 * downside is that servers must globally coordinate changes to subscriptions because the consequences of using
 * out-of-date cached subscription metadata are pretty severe.
 */
public class CachingSubscriptionDAO implements SubscriptionDAO {

    private static final String SUBSCRIPTIONS = "subscriptions";

    private final SubscriptionDAO _delegate;
    private final LoadingCache<String, Map<String, OwnedSubscription>> _cache;
    private final CacheHandle _cacheHandle;

    @Inject
    public CachingSubscriptionDAO(@CachingSubscriptionDAODelegate SubscriptionDAO delegate,
                                  @CachingSubscriptionDAORegistry CacheRegistry cacheRegistry) {
        _delegate = checkNotNull(delegate, "delegate");

        // The subscription cache has only a single value.  Use it for (a) expiration, (b) dropwizard cache clearing.
        _cache = CacheBuilder.newBuilder().
                expireAfterAccess(10, TimeUnit.MINUTES).
                recordStats().
                build(new CacheLoader<String, Map<String, OwnedSubscription>>() {
                    @Override
                    public Map<String, OwnedSubscription> load(String ignored) throws Exception {
                        return indexByName(_delegate.getAllSubscriptions());
                    }
                });
        _cacheHandle = cacheRegistry.register("subscriptions", _cache, true);
    }

    private Map<String, OwnedSubscription> indexByName(Collection<OwnedSubscription> subscriptions) {
        return Maps.uniqueIndex(subscriptions, new Function<Subscription, String>() {
            @Override
            public String apply(Subscription subscription) {
                return subscription.getName();
            }
        });
    }

    @Override
    public void insertSubscription(String ownerId, String subscription, Condition tableFilter, Duration subscriptionTtl,
                                   Duration eventTtl) {
        _delegate.insertSubscription(ownerId, subscription, tableFilter, subscriptionTtl, eventTtl);

        // Synchronously tell every other server in the cluster to forget what it has cached about subscriptions.
        _cacheHandle.invalidate(InvalidationScope.DATA_CENTER, SUBSCRIPTIONS);
    }

    @Override
    public void deleteSubscription(String subscription) {
        _delegate.deleteSubscription(subscription);

        // Synchronously tell every other server in the cluster to forget what it has cached about subscriptions.
        _cacheHandle.invalidate(InvalidationScope.DATA_CENTER, SUBSCRIPTIONS);
    }

    @Override
    public OwnedSubscription getSubscription(String subscription) {
        return _cache.getUnchecked(SUBSCRIPTIONS).get(subscription);
    }
    @Override
    public Collection<OwnedSubscription> getAllSubscriptions() {
        return _cache.getUnchecked(SUBSCRIPTIONS).values();
    }
}
