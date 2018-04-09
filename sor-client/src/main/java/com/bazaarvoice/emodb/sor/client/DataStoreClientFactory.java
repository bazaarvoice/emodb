package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.client.EmoClient;

public class DataStoreClientFactory extends AbstractDataStoreClientFactory {
    /**
     * Connects to the System of Record using the specified Emo client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Emo Jersey client.
     */
    public static DataStoreClientFactory forClusterAndEmoClient(String clusterName, EmoClient client) {
        return new DataStoreClientFactory(clusterName, client);
    }

    public static String getServiceName(String clusterName) {
        return AbstractDataStoreClientFactory.getServiceName(clusterName);
    }

    private DataStoreClientFactory(String clusterName, EmoClient emoClient) {
        super(clusterName, emoClient);
    }
}
