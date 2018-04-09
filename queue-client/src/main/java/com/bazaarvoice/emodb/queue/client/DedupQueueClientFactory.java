package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.client.EmoClient;

public class DedupQueueClientFactory extends AbstractDedupQueueClientFactory {

    /**
     * Connects to the DedupQueueService using the specified Emo client.  If you're using Dropwizard, use this
     * factory method and pass a JerseyEmoClient.
     */
    public static DedupQueueClientFactory forClusterAndEmoClient(String clusterName, EmoClient client) {
        return new DedupQueueClientFactory(clusterName, client);
    }

    public static String getServiceName(String clusterName) {
        return AbstractDedupQueueClientFactory.getServiceName(clusterName);
    }

    private DedupQueueClientFactory(String clusterName, EmoClient emoClient) {
        super(clusterName, emoClient);
    }
}
