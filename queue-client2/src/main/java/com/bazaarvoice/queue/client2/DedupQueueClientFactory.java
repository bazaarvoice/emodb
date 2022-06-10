package com.bazaarvoice.queue.client2;

import com.bazaarvoice.emodb.common.jersey2.Jersey2EmoClient;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.client.Client;
import java.net.URI;

public class DedupQueueClientFactory extends AbstractDedupQueueClientFactory {

    protected DedupQueueClientFactory(Client client, URI endPoint) {
        super(new Jersey2EmoClient(client), endPoint);
    }

    /**
     * Connects to the DedupQueueService using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static DedupQueueClientFactory forClusterAndHttpClient(Client client, URI endpoint) {
        client.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
        return new DedupQueueClientFactory(client, endpoint);
    }

}
