package com.bazaarvoice.emodb.blob.jaxrs.client;

import com.bazaarvoice.emodb.blob.client.AbstractBlobStoreClientFactory;
import com.bazaarvoice.emodb.common.jaxrs.JaxRSEmoClient;

import javax.ws.rs.client.Client;

public class BlobStoreClientFactory extends AbstractBlobStoreClientFactory {

    public BlobStoreClientFactory(String clusterName, Client client) {
        super(clusterName, new JaxRSEmoClient(client));
    }
}
