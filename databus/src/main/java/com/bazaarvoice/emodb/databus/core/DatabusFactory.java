package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.ReplaySubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;
import com.bazaarvoice.emodb.sor.condition.Condition;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * When used internally within the EmoDB server databus operations require the context of the owner which is making each
 * databus request as provided by {@link OwnerAwareDatabus}.  However, it is inconvenient to use nominally
 * different interfaces throughout the system and to require passing around an owner ID throughout the stack in order
 * to use OwnerAwareDatabus.
 *
 * The purpose of DatabusFactory is to provide a proxy for providing {@link Databus} interface access to the
 * OwnerAwareDatabus based on the owner ID passed to {@link #forOwner(String)}.
 */
public class DatabusFactory {

    private final OwnerAwareDatabus _ownerAwareDatabus;

    @Inject
    public DatabusFactory(OwnerAwareDatabus ownerAwareDatabus) {
        _ownerAwareDatabus = ownerAwareDatabus;
    }

    public Databus forOwner(final String ownerId) {
        checkNotNull(ownerId, "ownerId");

        /**
         * Proxy class for Databus that simply inserts the owner ID where appropriate.
         */
        return new Databus() {
            @Override
            public Iterator<Subscription> listSubscriptions(@Nullable String fromSubscriptionExclusive, long limit) {
                return _ownerAwareDatabus.listSubscriptions(ownerId, fromSubscriptionExclusive, limit);
            }

            @Override
            public void subscribe(String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl) {
                _ownerAwareDatabus.subscribe(ownerId, subscription, tableFilter, subscriptionTtl, eventTtl);
            }

            @Override
            public void subscribe(String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl, boolean ignoreSuppressedEvents) {
                _ownerAwareDatabus.subscribe(ownerId, subscription, tableFilter, subscriptionTtl, eventTtl, ignoreSuppressedEvents);
            }

            @Override
            public void unsubscribe(String subscription) {
                _ownerAwareDatabus.unsubscribe(ownerId, subscription);
            }

            @Override
            public Subscription getSubscription(String subscription) throws UnknownSubscriptionException {
                return _ownerAwareDatabus.getSubscription(ownerId, subscription);
            }

            @Override
            public long getEventCount(String subscription) {
                return _ownerAwareDatabus.getEventCount(ownerId, subscription);
            }

            @Override
            public long getEventCountUpTo(String subscription, long limit) {
                return _ownerAwareDatabus.getEventCountUpTo(ownerId, subscription, limit);
            }

            @Override
            public long getClaimCount(String subscription) {
                return _ownerAwareDatabus.getClaimCount(ownerId, subscription);
            }

            @Override
            public List<Event> peek(String subscription, int limit) {
                return _ownerAwareDatabus.peek(ownerId, subscription, limit);
            }

            @Override
            public List<Event> poll(String subscription, Duration claimTtl, int limit) {
                return _ownerAwareDatabus.poll(ownerId, subscription, claimTtl, limit);
            }

            @Override
            public void renew(String subscription, Collection<String> eventKeys, Duration claimTtl) {
                _ownerAwareDatabus.renew(ownerId, subscription, eventKeys, claimTtl);
            }

            @Override
            public void acknowledge(String subscription, Collection<String> eventKeys) {
                _ownerAwareDatabus.acknowledge(ownerId, subscription, eventKeys);
            }

            @Override
            public String replayAsync(String subscription) {
                return _ownerAwareDatabus.replayAsync(ownerId, subscription);
            }

            @Override
            public String replayAsyncSince(String subscription, Date since) {
                return _ownerAwareDatabus.replayAsyncSince(ownerId, subscription, since);
            }

            @Override
            public ReplaySubscriptionStatus getReplayStatus(String reference) {
                return _ownerAwareDatabus.getReplayStatus(ownerId, reference);
            }

            @Override
            public String moveAsync(String from, String to) {
                return _ownerAwareDatabus.moveAsync(ownerId, from, to);
            }

            @Override
            public MoveSubscriptionStatus getMoveStatus(String reference) {
                return _ownerAwareDatabus.getMoveStatus(ownerId, reference);
            }

            @Override
            public void injectEvent(String subscription, String table, String key) {
                _ownerAwareDatabus.injectEvent(ownerId, subscription, table, key);
            }

            @Override
            public void unclaimAll(String subscription) {
                _ownerAwareDatabus.unclaimAll(ownerId, subscription);
            }

            @Override
            public void purge(String subscription) {
                _ownerAwareDatabus.purge(ownerId, subscription);
            }
        };
    }
}
