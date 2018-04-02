package com.bazaarvoice.emodb.databus.jaxrs.client;

import com.bazaarvoice.emodb.common.jaxrs.JaxRSEmoClient;
import com.bazaarvoice.emodb.databus.client.AbstractDatabusClientFactory;

import javax.ws.rs.client.Client;

public class DatabusClientFactory extends AbstractDatabusClientFactory {

    private DatabusClientFactory(String clusterName, Client client) {
        super(clusterName, new JaxRSEmoClient(client));
    }

    public static DatabusClientFactory forClusterAndClient(String clusterName, Client client) {
        return new DatabusClientFactory(clusterName, client);
    }
}
