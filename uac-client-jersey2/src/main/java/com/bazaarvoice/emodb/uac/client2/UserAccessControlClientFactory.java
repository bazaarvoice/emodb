package com.bazaarvoice.emodb.uac.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.Jersey2EmoClient;
import com.bazaarvoice.emodb.client.RetryPolicy;
import com.bazaarvoice.emodb.uac.api.AuthUserAccessControl;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.client.Client;
import java.net.URI;

/**
 * Simple Factory for creating {@link AuthUserAccessControl} REST clients independent of Ostrich library.
 */
public class UserAccessControlClientFactory {
    private final EmoClient _client;
    private final URI _uri;

    private UserAccessControlClientFactory(URI endPoint, Client jerseyClient) {
        _uri = endPoint;
        _client = new Jersey2EmoClient(jerseyClient);
    }

    public static UserAccessControlClientFactory forClusterAndHttpClient(URI endPoint, Client jerseyClient) {
        jerseyClient.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
        return new UserAccessControlClientFactory(endPoint, jerseyClient);
    }

    public AuthUserAccessControl create() {
        return new UserAccessControlClient(_uri, _client, RetryPolicy.createDefault());
    }
}
