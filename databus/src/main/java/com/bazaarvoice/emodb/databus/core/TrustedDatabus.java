package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.auth.proxy.Credential;
import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.databus.api.ReplaySubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;
import com.bazaarvoice.emodb.sor.condition.Condition;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of AuthDatabus that defers all calls to a {@link Databus} which doesn't require authentication.
 * This class is useful when the user has already been authenticated and the calling method needs to forward control to
 * either a client on another server (and therefore requires authentication on that server) or an internal implementation
 * (which does not require authentication).
 */
public class TrustedDatabus implements AuthDatabus {
    
    private final Databus _databus;

    public TrustedDatabus(Databus databus) {
        _databus = checkNotNull(databus, "databus");
    }

    public Iterator<Subscription> listSubscriptions(@Credential String apiKey, @Nullable String fromSubscriptionExclusive, long limit) {
        return _databus.listSubscriptions(fromSubscriptionExclusive, limit);
    }

    public void renew(@Credential String apiKey, String subscription, Collection<String> eventKeys, Duration claimTtl) {
        _databus.renew(subscription, eventKeys, claimTtl);
    }

    public String moveAsync(@Credential String apiKey, String from, String to) {
        return _databus.moveAsync(from, to);
    }

    public List<Event> peek(@Credential String apiKey, String subscription, int limit) {
        return _databus.peek(subscription, limit);
    }

    public void acknowledge(@Credential String apiKey, String subscription, Collection<String> eventKeys) {
        _databus.acknowledge(subscription, eventKeys);
    }

    public long getEventCountUpTo(@Credential String apiKey, String subscription, long limit) {
        return _databus.getEventCountUpTo(subscription, limit);
    }

    @Override
    public String replayAsync(@Credential String apiKey, String subscription) {
        return _databus.replayAsync(subscription);
    }

    @Override
    public String replayAsyncSince(@Credential String apiKey, String subscription, Date since) {
        return _databus.replayAsyncSince(subscription, since);
    }

    @Override
    public ReplaySubscriptionStatus getReplayStatus(@Credential String apiKey, String reference) {
        return _databus.getReplayStatus(reference);
    }

    public void subscribe(@Credential String apiKey, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl) {
        subscribe(apiKey, subscription, tableFilter, subscriptionTtl, eventTtl, true);
    }

    @Override
    public void subscribe(@Credential String apiKey, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl, boolean includeDefaultJoinFilter) {
        _databus.subscribe(subscription, tableFilter, subscriptionTtl, eventTtl, includeDefaultJoinFilter);
    }

    public void purge(@Credential String apiKey, String subscription) {
        _databus.purge(subscription);
    }

    public void unsubscribe(@Credential String apiKey, String subscription) {
        _databus.unsubscribe(subscription);
    }

    public Subscription getSubscription(@Credential String apiKey, String subscription)
            throws UnknownSubscriptionException {
        return _databus.getSubscription(subscription);
    }

    public MoveSubscriptionStatus getMoveStatus(@Credential String apiKey, String reference) {
        return _databus.getMoveStatus(reference);
    }

    public void injectEvent(@Credential String apiKey, String subscription, String table, String key) {
        _databus.injectEvent(subscription, table, key);
    }

    public PollResult poll(@Credential String apiKey, String subscription, Duration claimTtl, int limit) {
        return _databus.poll(subscription, claimTtl, limit);
    }

    public long getClaimCount(@Credential String apiKey, String subscription) {
        return _databus.getClaimCount(subscription);
    }

    public void unclaimAll(@Credential String apiKey, String subscription) {
        _databus.unclaimAll(subscription);
    }

    public long getEventCount(@Credential String apiKey, String subscription) {
        return _databus.getEventCount(subscription);
    }
}
