package com.bazaarvoice.emodb.event.api;

import java.time.Duration;

public interface ChannelConfiguration {

    Duration getEventTtl(String channel);
}
