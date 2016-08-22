package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.client.DatabusClientFactory;
import com.sun.jersey.api.client.Client;

import java.net.URI;

public class DatabusRelayClientFactory extends DatabusClientFactory {

    public static DatabusRelayClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new DatabusRelayClientFactory(clusterName, client);
    }

    public DatabusRelayClientFactory(String clusterName, Client jerseyClient) {
        super(clusterName, jerseyClient);
    }

    @Override
    protected AuthDatabus create(URI endPointUri, boolean partitionSafe, EmoClient jerseyClient) {
        return new DatabusRelayClient(endPointUri, partitionSafe, jerseyClient);
    }
}
