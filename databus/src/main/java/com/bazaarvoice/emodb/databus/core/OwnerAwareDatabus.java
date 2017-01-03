package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.databus.api.ReplaySubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.api.UnauthorizedSubscriptionException;
import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;
import com.bazaarvoice.emodb.sor.condition.Condition;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * Parallel interface for {@link com.bazaarvoice.emodb.databus.api.Databus} that includes the owner's internal ID
 * with each request.  This class is intended for internal use only and should not be exposed outside the databus
 * module.  External systems that require a databus connection should get one using
 * {@link DatabusFactory#forOwner(String)}.
 */
public interface OwnerAwareDatabus {

    Iterator<Subscription> listSubscriptions(String ownerId, @Nullable String fromSubscriptionExclusive, long limit);

    void subscribe(String ownerId, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl)
        throws UnauthorizedSubscriptionException;

    @Deprecated
    void subscribe(String ownerId, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl, boolean ignoreSuppressedEvents)
        throws UnauthorizedSubscriptionException;

    void unsubscribe(String ownerId, String subscription)
        throws UnauthorizedSubscriptionException;

    Subscription getSubscription(String ownerId, String subscription)
            throws UnknownSubscriptionException, UnauthorizedSubscriptionException;

    long getEventCount(String ownerId, String subscription)
        throws UnauthorizedSubscriptionException;

    long getEventCountUpTo(String ownerId, String subscription, long limit)
        throws UnauthorizedSubscriptionException;

    long getClaimCount(String ownerId, String subscription)
        throws UnauthorizedSubscriptionException;

    List<Event> peek(String ownerId, String subscription, int limit)
        throws UnauthorizedSubscriptionException;

    PollResult poll(String ownerId, String subscription, Duration claimTtl, int limit)
        throws UnauthorizedSubscriptionException;

    void renew(String ownerId, String subscription, Collection<String> eventKeys, Duration claimTtl)
        throws UnauthorizedSubscriptionException;

    void acknowledge(String ownerId, String subscription, Collection<String> eventKeys)
        throws UnauthorizedSubscriptionException;

    String replayAsync(String ownerId, String subscription)
        throws UnauthorizedSubscriptionException;

    String replayAsyncSince(String ownerId, String subscription, Date since)
        throws UnauthorizedSubscriptionException;

    ReplaySubscriptionStatus getReplayStatus(String ownerId, String reference)
        throws UnauthorizedSubscriptionException;

    String moveAsync(String ownerId, String from, String to)
        throws UnauthorizedSubscriptionException;

    MoveSubscriptionStatus getMoveStatus(String ownerId, String reference)
        throws UnauthorizedSubscriptionException;

    void injectEvent(String ownerId, String subscription, String table, String key)
        throws UnauthorizedSubscriptionException;

    void unclaimAll(String ownerId, String subscription)
        throws UnauthorizedSubscriptionException;

    void purge(String ownerId, String subscription)
        throws UnauthorizedSubscriptionException;
}
