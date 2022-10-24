package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.Jersey2EmoClient;
import com.bazaarvoice.emodb.client.RetryPolicy;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.client.Client;
import java.net.URI;

/**
 * Connects to the System of Record using Jersey 2.x client.
 */
public class DataStoreClientFactory {
    private final EmoClient _client;
    private final URI _uri;

    public static DataStoreClientFactory forClusterAndHttpClient(URI endPoint, Client client) {
        client.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
        return new DataStoreClientFactory(endPoint, client);
    }

    private DataStoreClientFactory(URI endPoint, Client jerseyClient) {
        _uri = endPoint;
        _client = new Jersey2EmoClient(jerseyClient);
    }

    public AuthDataStore create() {
        return new DataStoreClient(_uri, _client, RetryPolicy.createDefault());
    }
}
