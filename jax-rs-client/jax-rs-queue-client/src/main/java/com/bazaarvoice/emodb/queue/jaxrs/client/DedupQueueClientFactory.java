package com.bazaarvoice.emodb.queue.jaxrs.client;

import com.bazaarvoice.emodb.common.jaxrs.JaxRSEmoClient;
import com.bazaarvoice.emodb.queue.client.AbstractDedupQueueClientFactory;

import javax.ws.rs.client.Client;

public class DedupQueueClientFactory extends AbstractDedupQueueClientFactory {
    private DedupQueueClientFactory(String clusterName, Client client) {
        super(clusterName, new JaxRSEmoClient(client));
    }

    public static DedupQueueClientFactory forClusterAndClient(String clusterName, Client client) {
        return new DedupQueueClientFactory(clusterName, client);
    }
}
