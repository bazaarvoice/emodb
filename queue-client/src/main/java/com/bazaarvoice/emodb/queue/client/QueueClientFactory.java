package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.client.Client;

public class QueueClientFactory extends AbstractQueueClientFactory {

    /**
     * Connects to the QueueService using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static QueueClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        client.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
        return new QueueClientFactory(clusterName, client);
    }

    private QueueClientFactory(String clusterName, Client jerseyClient) {
        super(clusterName, new JerseyEmoClient(jerseyClient));
    }

}
