package com.bazaarvoice.emodb.blob.client;

import com.bazaarvoice.emodb.client.EmoClient;

public class BlobStoreClientFactory extends AbstractBlobStoreClientFactory {

    /**
     * Connects to the Blob Store using the specified Emo client.  If you're using Dropwizard, use this
     * constructor and pass the Dropwizard-constructed Emo Jersey client.
     */
    public static BlobStoreClientFactory forClusterAndEmoClient(String clusterName, EmoClient client) {
        return new BlobStoreClientFactory(clusterName, client);
    }

    public static String getServiceName(String clusterName) {
        return AbstractBlobStoreClientFactory.getServiceName(clusterName);
    }

    private BlobStoreClientFactory(String clusterName, EmoClient emoClient) {
        super(clusterName, emoClient);
    }
}
