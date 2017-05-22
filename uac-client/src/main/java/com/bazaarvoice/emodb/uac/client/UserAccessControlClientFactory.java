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
import com.sun.jersey.api.client.Client;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.client.apache4.ApacheHttpClient4Handler;
import com.sun.jersey.client.apache4.config.ApacheHttpClient4Config;
import com.sun.jersey.client.apache4.config.DefaultApacheHttpClient4Config;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.client.HttpClientConfiguration;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.jackson.JacksonMessageBodyProvider;
import io.dropwizard.util.Duration;
import org.apache.http.client.HttpClient;

import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import java.net.URI;

/**
 * Factory constructor for creating {@link UserAccessControl} REST clients.
 */
public class UserAccessControlClientFactory implements MultiThreadedServiceFactory<AuthUserAccessControl> {

    private static ValidatorFactory _validatorFactory = Validation.buildDefaultValidatorFactory();

    private final String _clusterName;
    private final EmoClient _client;

    public static UserAccessControlClientFactory forCluster(String clusterName, MetricRegistry metricRegistry) {
        HttpClientConfiguration httpClientConfiguration = new HttpClientConfiguration();
        httpClientConfiguration.setKeepAlive(Duration.seconds(1));
        return new UserAccessControlClientFactory(clusterName, createDefaultJerseyClient(httpClientConfiguration, getServiceName(clusterName), metricRegistry));
    }

    /**
     * Connects to the User Access Control service using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static UserAccessControlClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new UserAccessControlClientFactory(clusterName, client);
    }

    public static UserAccessControlClientFactory forClusterAndHttpConfiguration(String clusterName, HttpClientConfiguration configuration, MetricRegistry metricRegistry) {
        return new UserAccessControlClientFactory(clusterName, createDefaultJerseyClient(configuration, getServiceName(clusterName), metricRegistry));
    }

    private static ApacheHttpClient4 createDefaultJerseyClient(HttpClientConfiguration configuration, String serviceName, MetricRegistry metricRegistry) {
        HttpClient httpClient = new HttpClientBuilder(metricRegistry).using(configuration).build(serviceName);
        ApacheHttpClient4Handler handler = new ApacheHttpClient4Handler(httpClient, null, true);
        ApacheHttpClient4Config config = new DefaultApacheHttpClient4Config();
        config.getSingletons().add(new JacksonMessageBodyProvider(Jackson.newObjectMapper(), _validatorFactory.getValidator()));
        return new ApacheHttpClient4(handler, config);
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