package com.bazaarvoice.emodb.blob.jaxrs.client;

import com.bazaarvoice.emodb.blob.client.AbstractBlobStoreClientFactory;
import com.bazaarvoice.emodb.common.jaxrs.JaxRSEmoClient;

import javax.ws.rs.client.Client;
import java.util.concurrent.ScheduledExecutorService;

public class BlobStoreClientFactory extends AbstractBlobStoreClientFactory {

    private BlobStoreClientFactory(String clusterName, Client client) {
        super(clusterName, new JaxRSEmoClient(client));
    }

    public static BlobStoreClientFactory forClusterAndClient(String clusterName, Client client) {
        return new BlobStoreClientFactory(clusterName, client);
    }

    public BlobStoreClientFactory withConnectionManagementService(ScheduledExecutorService service) {
        setConnectionManagementService(service);
        return this;
    }
}
