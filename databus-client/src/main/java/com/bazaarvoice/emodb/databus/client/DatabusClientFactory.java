package com.bazaarvoice.emodb.databus.client;

import com.bazaarvoice.emodb.client.EmoClient;

public class DatabusClientFactory extends AbstractDatabusClientFactory {

    /**
     * Connects to the Databus using the specified Emo client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Emo Jersey client.
     */
    public static DatabusClientFactory forClusterAndEmoClient(String clusterName, EmoClient client) {
        return new DatabusClientFactory(clusterName, client);
    }

    public static String getServiceName(String clusterName) {
        return AbstractDatabusClientFactory.getServiceName(clusterName);
    }

    protected DatabusClientFactory(String clusterName, EmoClient emoClient) {
        super(clusterName, emoClient);
    }
}
