package com.bazaarvoice.emodb.databus.jaxrs.client;

import com.bazaarvoice.emodb.common.jaxrs.JaxRSEmoClient;
import com.bazaarvoice.emodb.databus.client.AbstractDatabusClientFactory;

import javax.ws.rs.client.Client;

public class DatabusClientFactory extends AbstractDatabusClientFactory {
    public DatabusClientFactory(String clusterName, Client client) {
        super(clusterName, new JaxRSEmoClient(client));
    }
}
