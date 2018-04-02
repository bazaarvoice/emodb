package com.bazaarvoice.emodb.queue.jaxrs.client;

import com.bazaarvoice.emodb.common.jaxrs.JaxRSEmoClient;
import com.bazaarvoice.emodb.queue.client.AbstractQueueClientFactory;

import javax.ws.rs.client.Client;

public class QueueClientFactory extends AbstractQueueClientFactory {

    private QueueClientFactory(String clusterName, Client client) {
        super(clusterName, new JaxRSEmoClient(client));
    }

    public static QueueClientFactory forClusterAndClient(String clusterName, Client client) {
        return new QueueClientFactory(clusterName, client);
    }
}
