package com.bazaarvoice.emodb.blob.client;

import com.bazaarvoice.emodb.client.Jersey2EmoClient;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.client.Client;
import java.net.URI;


public class BlobStoreClientFactory extends AbstractBlobStoreClientFactory {

    /**
     * Connects to the Blob Store using the specified Jersey client.  If you're using Dropwizard, use this
     * constructor and pass the Dropwizard-constructed Jersey client.
     */
    public static BlobStoreClientFactory forClusterAndHttpClient(Client client, URI endpoint) {
        client.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
        return new BlobStoreClientFactory(client, endpoint);
    }

    private BlobStoreClientFactory(Client jerseyClient, URI endpoint) {
        super(new Jersey2EmoClient(jerseyClient), endpoint);
    }
}
