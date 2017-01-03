package com.bazaarvoice.emodb.databus.client;

import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.databus.api.ReplaySubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.ostrich.partition.PartitionKey;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Databus instance that takes an {@link AuthDatabus} and API key and proxies all calls using the API key.
 *
 * Note: The {@link PartitionKey} annotations must match those from AuthDatabus.
 */
class DatabusAuthenticatorProxy implements Databus {
    
    private final AuthDatabus _authDatabus;
    private final String _apiKey;

    DatabusAuthenticatorProxy(AuthDatabus authDatabus, String apiKey) {
        _authDatabus = authDatabus;
        _apiKey = apiKey;
    }

    @Override
    public Iterator<Subscription> listSubscriptions(@Nullable String fromSubscriptionExclusive, long limit) {
        return _authDatabus.listSubscriptions(_apiKey, fromSubscriptionExclusive, limit);
    }

    @Override
    public long getEventCount(@PartitionKey String subscription) {
        return _authDatabus.getEventCount(_apiKey, subscription);
    }

    @Override
    public void purge(@PartitionKey String subscription) {
        _authDatabus.purge(_apiKey, subscription);
    }

    @Override
    public MoveSubscriptionStatus getMoveStatus(String reference) {
        return _authDatabus.getMoveStatus(_apiKey, reference);
    }

    @Override
    public Subscription getSubscription(String subscription)
            throws UnknownSubscriptionException {
        return _authDatabus.getSubscription(_apiKey, subscription);
    }

    @Override
    public void renew(@PartitionKey String subscription, Collection<String> eventKeys, Duration claimTtl) {
        _authDatabus.renew(_apiKey, subscription, eventKeys, claimTtl);
    }

    @Override
    public String moveAsync(String from, String to) {
        return _authDatabus.moveAsync(_apiKey, from, to);
    }

    @Override
    public void unclaimAll(@PartitionKey String subscription) {
        _authDatabus.unclaimAll(_apiKey, subscription);
    }

    @Override
    public PollResult poll(@PartitionKey String subscription, Duration claimTtl, int limit) {
        return _authDatabus.poll(_apiKey, subscription, claimTtl, limit);
    }

    @Override
    public void acknowledge(@PartitionKey String subscription, Collection<String> eventKeys) {
        _authDatabus.acknowledge(_apiKey, subscription, eventKeys);
    }

    @Override
    public void unsubscribe(@PartitionKey String subscription) {
        _authDatabus.unsubscribe(_apiKey, subscription);
    }

    @Override
    public List<Event> peek(@PartitionKey String subscription, int limit) {
        return _authDatabus.peek(_apiKey, subscription, limit);
    }

    @Override
    public long getEventCountUpTo(@PartitionKey String subscription, long limit) {
        return _authDatabus.getEventCountUpTo(_apiKey, subscription, limit);
    }

    @Override
    public void subscribe(String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl) {
        subscribe(subscription, tableFilter, subscriptionTtl, eventTtl, true);
    }

    @Override
    public void subscribe(String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl, boolean includeDefaultJoinFilter) {
        _authDatabus.subscribe(_apiKey, subscription, tableFilter, subscriptionTtl, eventTtl, includeDefaultJoinFilter);
    }

    @Override
    public long getClaimCount(@PartitionKey String subscription) {
        return _authDatabus.getClaimCount(_apiKey, subscription);
    }

    @Override
    public String replayAsync(String subscription) {
        return _authDatabus.replayAsync(_apiKey, subscription);
    }

    @Override
    public String replayAsyncSince(String subscription, Date since) {
        return _authDatabus.replayAsyncSince(_apiKey, subscription, since);
    }

    @Override
    public ReplaySubscriptionStatus getReplayStatus(String reference) {
        return _authDatabus.getReplayStatus(_apiKey, reference);
    }

    @Override
    public void injectEvent(String subscription, String table, String key) {
        _authDatabus.injectEvent(_apiKey, subscription, table, key);
    }

    AuthDatabus getProxiedInstance() {
        return _authDatabus;
    }
}
