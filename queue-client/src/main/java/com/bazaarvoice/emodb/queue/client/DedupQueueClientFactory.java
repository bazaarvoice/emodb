package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.util.Duration;
import javax.ws.rs.client.Client;
import org.glassfish.jersey.client.ClientProperties;

public class DedupQueueClientFactory extends AbstractDedupQueueClientFactory {

    /**
     * Connects to the DedupQueueService using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static DedupQueueClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        client.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
        return new DedupQueueClientFactory(clusterName, client);
    }

    private DedupQueueClientFactory(String clusterName, Client jerseyClient) {
        super(clusterName, new JerseyEmoClient(jerseyClient));
    }
}
