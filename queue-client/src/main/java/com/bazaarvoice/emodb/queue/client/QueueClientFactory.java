package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.client.EmoClient;

public class QueueClientFactory extends AbstractQueueClientFactory {

    /**
     * Connects to the QueueService using the specified Emo client.  If you're using Dropwizard, use this
     * factory method and pass a JerseyEmoClient.
     */
    public static QueueClientFactory forClusterAndEmoClient(String clusterName, EmoClient client) {
        return new QueueClientFactory(clusterName, client);
    }

    public static String getServiceName(String clusterName) {
        return AbstractQueueClientFactory.getServiceName(clusterName);
    }

    private QueueClientFactory(String clusterName, EmoClient emoClient) {
        super(clusterName, emoClient);
    }

}
