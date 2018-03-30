package com.bazaarvoice.emodb.queue.jaxrs.client;

import com.bazaarvoice.emodb.common.jaxrs.JaxRSEmoClient;
import com.bazaarvoice.emodb.queue.client.AbstractDedupQueueClientFactory;

import javax.ws.rs.client.Client;

public class DedupQueueClientFactory extends AbstractDedupQueueClientFactory {
    public DedupQueueClientFactory(String clusterName, Client client) {
        super(clusterName, new JaxRSEmoClient(client));
    }
}
