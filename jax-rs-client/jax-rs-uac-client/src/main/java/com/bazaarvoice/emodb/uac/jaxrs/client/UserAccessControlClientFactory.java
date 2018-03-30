package com.bazaarvoice.emodb.uac.jaxrs.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.common.dropwizard.discovery.Payload;
import com.bazaarvoice.emodb.common.dropwizard.discovery.ServiceNames;
import com.bazaarvoice.emodb.common.jaxrs.JaxRSEmoClient;
import com.bazaarvoice.emodb.common.json.JsonStreamingEOFException;
import com.bazaarvoice.emodb.uac.api.AuthUserAccessControl;
import com.bazaarvoice.emodb.uac.api.UserAccessControl;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.google.common.net.HttpHeaders;

//import javax.validation.Validation;
//import javax.validation.ValidatorFactory;
import javax.ws.rs.client.Client;
import java.net.URI;

/**
 * Factory constructor for creating {@link UserAccessControl} REST clients.
 */
public class UserAccessControlClientFactory implements MultiThreadedServiceFactory<AuthUserAccessControl> {

//    private static ValidatorFactory _validatorFactory = Validation.buildDefaultValidatorFactory();

    private final String _clusterName;
    private final EmoClient _client;

    public UserAccessControlClientFactory(String clusterName, Client client) {
        _clusterName = clusterName;
        _client = new JaxRSEmoClient(client);
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