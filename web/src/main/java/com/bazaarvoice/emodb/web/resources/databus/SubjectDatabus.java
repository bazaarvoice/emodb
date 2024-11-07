package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.databus.api.ReplaySubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;
import com.bazaarvoice.emodb.sor.condition.Condition;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

/**
 * Interface for providing owner-aware access to {@link com.bazaarvoice.emodb.databus.api.Databus} using an authenticated
 * subject.  Similar to {@link com.bazaarvoice.emodb.databus.api.AuthDatabus} except the authentication credential is
 * the subject and not an API key.  This is used to support the following functionality required by
 * {@link DatabusResource1}:
 *
 * <ul>
 *     <li>If the local server is the partition for the subscription then requests should be forwarded to a local
 *         databus implementation.  In this case the authentication credential is the subject's ID.</li>
 *     <li>If a remote server is the partition for the subscription then requests should be forwarded to that server
 *         using its databus REST API.  In this case the authentication credential is the subject's ID, which is the
 *         API key.</li>
 * </ul>
 *
 * By using the subject as the authenticator local and remote implementations can forward the request using the appropriate
 * subject identifier.
 *
 * This method should exactly mirror Databus except with added subject in each call.
 *
 * @see com.bazaarvoice.emodb.databus.api.Databus
 */
public interface SubjectDatabus {
    Iterator<Subscription> listSubscriptions(Subject subject, @Nullable String fromSubscriptionExclusive, long limit);

    void subscribe(Subject subject, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl);

    void subscribe(Subject subject, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl, boolean includeDefaultJoinFilter);

    void unsubscribe(Subject subject, String subscription);

    Subscription getSubscription(Subject subject, String subscription)
            throws UnknownSubscriptionException;

    long getEventCount(Subject subject, String subscription);

    long getEventCountUpTo(Subject subject, String subscription, long limit);

    long getMasterCount(Subject subject);

    long getClaimCount(Subject subject, String subscription);

    Iterator<Event> peek(Subject subject, String subscription, int limit);

    PollResult poll(Subject subject, String subscription, Duration claimTtl, int limit);

    void renew(Subject subject, String subscription, Collection<String> eventKeys, Duration claimTtl);

    void acknowledge(Subject subject, String subscription, Collection<String> eventKeys);

    String replayAsync(Subject subject, String subscription);

    String replayAsyncSince(Subject subject, String subscription, Date since);

    ReplaySubscriptionStatus getReplayStatus(Subject subject, String reference);

    String moveAsync(Subject subject, String from, String to);

    MoveSubscriptionStatus getMoveStatus(Subject subject, String reference);

    void injectEvent(Subject subject, String subscription, String table, String key);

    void unclaimAll(Subject subject, String subscription);

    void purge(Subject subject, String subscription);
}
