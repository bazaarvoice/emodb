package com.bazaarvoice.emodb.sor.jaxrs.client;

import com.bazaarvoice.emodb.common.jaxrs.JaxRSEmoClient;
import com.bazaarvoice.emodb.sor.client.AbstractDataStoreClientFactory;

import javax.ws.rs.client.Client;

public class DataStoreClientFactory extends AbstractDataStoreClientFactory {

    public DataStoreClientFactory(String clusterName, Client client) {
        super(clusterName, new JaxRSEmoClient(client));
    }

    public static DataStoreClientFactory forClusterAndClient(String clusterName, Client client) {
        return new DataStoreClientFactory(clusterName, client);
    }
}
