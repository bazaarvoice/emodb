package com.bazaarvoice.emodb.databus.db.generic;

import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.databus.model.DefaultOwnedSubscription;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.time.Clock;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Generic in-memory implementation of SubscriptionDAO.  Useful for unit testing.
 */
public class InMemorySubscriptionDAO implements SubscriptionDAO {

    private final ConcurrentMap<String, OwnedSubscription> _subscriptions = Maps.newConcurrentMap();
    private final Clock _clock;

    public InMemorySubscriptionDAO() {
        this(Clock.systemUTC());
    }

    public InMemorySubscriptionDAO(Clock clock) {
        _clock = clock;
    }

    @Override
    public void insertSubscription(String ownerId, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl) {
        _subscriptions.put(subscription, new DefaultOwnedSubscription(subscription, tableFilter,
                new Date(_clock.millis() + subscriptionTtl.toMillis()), eventTtl, ownerId));
    }

    @Override
    public void deleteSubscription(String subscription) {
        _subscriptions.remove(subscription);
    }

    @Nullable
    @Override
    public OwnedSubscription getSubscription(String subscription) {
        OwnedSubscription ownedSubscription = _subscriptions.get(subscription);
        if (ownedSubscription != null && _clock.millis() >= ownedSubscription.getExpiresAt().getTime()) {
            _subscriptions.remove(subscription, ownedSubscription);
            ownedSubscription = null;
        }
        return ownedSubscription;
    }

    @Override
    public Iterable<OwnedSubscription> getAllSubscriptions() {
        return _subscriptions.values().stream()
                .filter(subscription -> _clock.millis() < subscription.getExpiresAt().getTime())
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<String> getAllSubscriptionNames() {
        return Iterables.transform(getAllSubscriptions(), OwnedSubscription::getName);
    }
}
