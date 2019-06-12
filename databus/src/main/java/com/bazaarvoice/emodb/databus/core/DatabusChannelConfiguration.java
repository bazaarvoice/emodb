package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.databus.ChannelNames;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.db.SubscriptionDAO;
import com.bazaarvoice.emodb.event.api.ChannelConfiguration;
import com.google.inject.Inject;

import java.time.Duration;

import static com.google.common.base.Preconditions.checkNotNull;

public class DatabusChannelConfiguration implements ChannelConfiguration {
    private static final Duration FANOUT_TTL = Duration.ofDays(365);  // Basically forever
    public static final Duration CANARY_TTL = Duration.ofDays(365);  // Basically forever
    public static final Duration MEGABUS_TTL = Duration.ofDays(365); // Basically forever
    public static final Duration REPLAY_TTL = Duration.ofHours(50); // 2 days + 2 hours

    private final SubscriptionDAO _subscriptionDao;


    @Inject
    public DatabusChannelConfiguration(SubscriptionDAO subscriptionDao) {
        _subscriptionDao = checkNotNull(subscriptionDao);
    }

    @Override
    public Duration getEventTtl(String channel) {
        // Make sure the Fanout channel events basically live forever for eventual consistent delivery
        if (ChannelNames.isSystemFanoutChannel(channel)) {
            return FANOUT_TTL;
        }

        // Read channel?
        String queue = ChannelNames.dedupChannels().queueFromReadChannel(channel);
        if (queue != null) {
            Subscription subscription = _subscriptionDao.getSubscription(queue);
            if (subscription != null) {
                return subscription.getEventTtl();
            }
        }

        // Write channel?
        Subscription subscription = _subscriptionDao.getSubscription(channel);
        if (subscription != null) {
            return subscription.getEventTtl();
        }

        return Duration.ofSeconds(1);  // If the subscription has expired, expire events quickly.
    }
}
