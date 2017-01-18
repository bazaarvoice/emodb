package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.auth.jersey.Subject;
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
 * Base implementation for {@link SubjectDatabus}.
 *
 * Note: The {@link PartitionKey} annotations must match those from {@link com.bazaarvoice.emodb.databus.client.DatabusClient}.
 */
public abstract class AbstractSubjectDatabus implements SubjectDatabus {

    abstract protected Databus databus(Subject subject);

    @Override
    public Iterator<Subscription> listSubscriptions(Subject subject, @Nullable String fromSubscriptionExclusive, long limit) {
        return databus(subject).listSubscriptions(fromSubscriptionExclusive, limit);
    }

    @Override
    public void subscribe(Subject subject, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl) {
        databus(subject).subscribe(subscription, tableFilter, subscriptionTtl, eventTtl);
    }

    @Override
    public void subscribe(Subject subject, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl, boolean includeDefaultJoinFilter) {
        databus(subject).subscribe(subscription, tableFilter, subscriptionTtl, eventTtl, includeDefaultJoinFilter);
    }

    @Override
    public void unsubscribe(Subject subject, @PartitionKey String subscription) {
        databus(subject).unsubscribe(subscription);
    }

    @Override
    public Subscription getSubscription(Subject subject, String subscription) throws UnknownSubscriptionException {
        return databus(subject).getSubscription(subscription);
    }

    @Override
    public long getEventCount(Subject subject, @PartitionKey String subscription) {
        return databus(subject).getEventCount(subscription);
    }

    @Override
    public long getEventCountUpTo(Subject subject, @PartitionKey String subscription, long limit) {
        return databus(subject).getEventCountUpTo(subscription, limit);
    }

    @Override
    public long getClaimCount(Subject subject, @PartitionKey String subscription) {
        return databus(subject).getClaimCount(subscription);
    }

    @Override
    public List<Event> peek(Subject subject, @PartitionKey String subscription, int limit) {
        return databus(subject).peek(subscription, limit);
    }

    @Override
    public PollResult poll(Subject subject, @PartitionKey String subscription, Duration claimTtl, int limit) {
        return databus(subject).poll(subscription, claimTtl, limit);
    }

    @Override
    public void renew(Subject subject, @PartitionKey String subscription, Collection<String> eventKeys, Duration claimTtl) {
        databus(subject).renew(subscription, eventKeys, claimTtl);
    }

    @Override
    public void acknowledge(Subject subject, @PartitionKey String subscription, Collection<String> eventKeys) {
        databus(subject).acknowledge(subscription, eventKeys);
    }

    @Override
    public String replayAsync(Subject subject, String subscription) {
        return databus(subject).replayAsync(subscription);
    }

    @Override
    public String replayAsyncSince(Subject subject, String subscription, Date since) {
        return databus(subject).replayAsyncSince(subscription, since);
    }

    @Override
    public ReplaySubscriptionStatus getReplayStatus(Subject subject, String reference) {
        return databus(subject).getReplayStatus(reference);
    }

    @Override
    public String moveAsync(Subject subject, String from, String to) {
        return databus(subject).moveAsync(from, to);
    }

    @Override
    public MoveSubscriptionStatus getMoveStatus(Subject subject, String reference) {
        return databus(subject).getMoveStatus(reference);
    }

    @Override
    public void injectEvent(Subject subject, String subscription, String table, String key) {
        databus(subject).injectEvent(subscription, table, key);
    }

    @Override
    public void unclaimAll(Subject subject, @PartitionKey String subscription) {
        databus(subject).unclaimAll(subscription);
    }

    @Override
    public void purge(Subject subject, @PartitionKey String subscription) {
        databus(subject).purge(subscription);
    }
}
