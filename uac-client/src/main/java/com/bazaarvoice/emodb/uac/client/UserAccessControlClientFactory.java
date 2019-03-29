package com.bazaarvoice.emodb.uac.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.common.dropwizard.discovery.Payload;
import com.bazaarvoice.emodb.common.dropwizard.discovery.ServiceNames;
import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.bazaarvoice.emodb.common.json.JsonStreamingEOFException;
import com.bazaarvoice.emodb.uac.api.AuthUserAccessControl;
import com.bazaarvoice.emodb.uac.api.UserAccessControl;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.codahale.metrics.MetricRegistry;
import com.google.common.net.HttpHeaders;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.util.Duration;
import javax.ws.rs.client.Client;

import java.net.URI;
import org.glassfish.jersey.client.ClientProperties;

/**
 * Factory constructor for creating {@link UserAccessControl} REST clients.
 */
public class UserAccessControlClientFactory implements MultiThreadedServiceFactory<AuthUserAccessControl> {

    private final String _clusterName;
    private final EmoClient _client;

    /**
     * Connects to the User Access Control service using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static UserAccessControlClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        client.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
        return new UserAccessControlClientFactory(clusterName, client);
    }

    private UserAccessControlClientFactory(String clusterName, Client jerseyClient) {
        _clusterName = clusterName;
        _client = new JerseyEmoClient(jerseyClient);
    }

    protected static String getServiceName(String clusterName) {
        return ServiceNames.forNamespaceAndBaseServiceName(clusterName, UserAccessControlClient.BASE_SERVICE_NAME);
    }

    @Override
    public String getServiceName() {
        return getServiceName(_clusterName);
    }

    @Override
    public void configure(ServicePoolBuilder<AuthUserAccessControl> servicePoolBuilder) {
        // Defaults are ok
    }

    @Override
    public AuthUserAccessControl create(ServiceEndPoint endPoint) {
        Payload payload = Payload.valueOf(endPoint.getPayload());
        return new UserAccessControlClient(payload.getServiceUrl(), _client);
    }

    @Override
    public void destroy(ServiceEndPoint endPoint, AuthUserAccessControl service) {
        // Nothing to do
    }

    @Override
    public boolean isRetriableException(Exception e) {
        return (e instanceof EmoClientException &&
                ((EmoClientException) e).getResponse().getStatus() >= 500) ||
                e instanceof JsonStreamingEOFException;
    }

    @Override
    public boolean isHealthy(ServiceEndPoint endPoint) {
        URI adminUrl = Payload.valueOf(endPoint.getPayload()).getAdminUrl();
        return _client.resource(adminUrl).path("/healthcheck")
                .header(HttpHeaders.CONNECTION, "close")
                .head().getStatus() == 200;
    }

    public MultiThreadedServiceFactory<UserAccessControl> usingCredentials(final String apiKey) {
        final UserAccessControlClientFactory authServiceFactory = this;

        return new MultiThreadedServiceFactory<UserAccessControl>() {
            @Override
            public String getServiceName() {
                return authServiceFactory.getServiceName();
            }

            @Override
            public void configure(ServicePoolBuilder<UserAccessControl> servicePoolBuilder) {
                // Defaults are ok
            }

            @Override
            public UserAccessControl create(ServiceEndPoint endPoint) {
                AuthUserAccessControl AuthUserAccessControl = authServiceFactory.create(endPoint);
                return new UserAccessControlAuthenticatorProxy(AuthUserAccessControl, apiKey);
            }

            @Override
            public void destroy(ServiceEndPoint endPoint, UserAccessControl service) {
                // Nothing to do
            }

            @Override
            public boolean isHealthy(ServiceEndPoint endPoint) {
                return authServiceFactory.isHealthy(endPoint);
            }

            @Override
            public boolean isRetriableException(Exception exception) {
                return authServiceFactory.isRetriableException(exception);
            }
        };
    }
}