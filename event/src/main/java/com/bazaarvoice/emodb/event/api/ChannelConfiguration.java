package com.bazaarvoice.emodb.event.api;

import org.joda.time.Duration;

public interface ChannelConfiguration {

    Duration getEventTtl(String channel);
}
