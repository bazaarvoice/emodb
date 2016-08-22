package com.bazaarvoice.emodb.queue.core;

import com.bazaarvoice.emodb.event.api.ChannelConfiguration;
import org.joda.time.Duration;

public class QueueChannelConfiguration implements ChannelConfiguration {
    // Amazon SQS keeps messages for 14 days.  Seems like a reasonable choice...
    private static final Duration TTL = Duration.standardDays(14);

    @Override
    public Duration getEventTtl(String queue) {
        return TTL;
    }
}
