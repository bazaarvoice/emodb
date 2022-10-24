package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.client.Jersey2EmoClient;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.client.Client;
import java.net.URI;

public class QueueClientFactory extends AbstractQueueClientFactory {

    private QueueClientFactory(Client client, URI endPoint) {
        super(new Jersey2EmoClient(client), endPoint);
    }

    public static QueueClientFactory forClusterAndHttpClient(Client client, URI endpoint) {
        client.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
        return new QueueClientFactory(client, endpoint);
    }

}
