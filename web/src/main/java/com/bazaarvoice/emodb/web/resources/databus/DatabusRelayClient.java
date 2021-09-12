package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.databus.client.DatabusClient;

import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.time.Duration;

/**
 * Variant of the standard DatabusClient implementation to be used internally by the EmoDB server for forwarding requests
 * to other nodes.
 */
public class DatabusRelayClient extends DatabusClient {
    public DatabusRelayClient(URI endPoint, boolean partitionSafe, EmoClient jerseyClient) {
        super(endPoint, partitionSafe, jerseyClient);
    }

    @Override
    protected UriBuilder getPollUriBuilder(String subscription, Duration claimTtl, int limit) {
        // Use the same URI as our normal client, but append "ignoreLongPoll=true"
        return super.getPollUriBuilder(subscription, claimTtl, limit)
                .queryParam("ignoreLongPoll", true);
    }
}
