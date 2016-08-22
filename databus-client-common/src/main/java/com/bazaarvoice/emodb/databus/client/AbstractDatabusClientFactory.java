package com.bazaarvoice.emodb.databus.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.common.dropwizard.discovery.Payload;
import com.bazaarvoice.emodb.common.dropwizard.discovery.ServiceNames;
import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.partition.ConsistentHashPartitionFilter;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.google.common.net.HttpHeaders;

import java.net.URI;

/**
 * Abstract parent class for databus clients.  Subclasses are expected to create and configure an
 * {@link EmoClient} and then supply it via the constructor.
 */
abstract public class AbstractDatabusClientFactory implements MultiThreadedServiceFactory<AuthDatabus> {

    private final String _clusterName;
    private final EmoClient _client;

    protected AbstractDatabusClientFactory(String clusterName, EmoClient client) {
        _clusterName = clusterName;
        _client = client;
    }

    @Override
    public String getServiceName() {
        return getServiceName(_clusterName);
    }

    protected static String getServiceName(String clusterName) {
        return ServiceNames.forNamespaceAndBaseServiceName(clusterName, DatabusClient.BASE_SERVICE_NAME);
    }

    @Override
    public void configure(ServicePoolBuilder<AuthDatabus> servicePoolBuilder) {
        servicePoolBuilder.withPartitionFilter(new ConsistentHashPartitionFilter())
                .withPartitionContextAnnotationsFrom(DatabusClient.class);
    }

    @Override
    public AuthDatabus create(ServiceEndPoint endPoint) {
        Payload payload = Payload.valueOf(endPoint.getPayload());
        boolean partitionSafe = Boolean.FALSE.equals(payload.getExtensions().get("proxy"));
        return create(payload.getServiceUrl(), partitionSafe, _client);
    }

    protected AuthDatabus create(URI endPointUri, boolean partitionSafe, EmoClient jerseyClient) {
        return new DatabusClient(endPointUri, partitionSafe, jerseyClient);
    }

    @Override
    public void destroy(ServiceEndPoint endPoint, AuthDatabus service) {
        // Nothing to do
    }

    @Override
    public boolean isRetriableException(Exception e) {
        return (e instanceof EmoClientException &&
                ((EmoClientException) e).getResponse().getStatus() >= 500);
    }

    @Override
    public boolean isHealthy(ServiceEndPoint endPoint) {
        URI adminUrl = Payload.valueOf(endPoint.getPayload()).getAdminUrl();
        return _client.resource(adminUrl).path("/healthcheck")
                .header(HttpHeaders.CONNECTION, "close")
                .head().getStatus() == 200;
    }

    public MultiThreadedServiceFactory<Databus> usingCredentials(final String apiKey) {
        final AbstractDatabusClientFactory authServiceFactory = this;

        return new MultiThreadedServiceFactory<Databus>() {
            @Override
            public String getServiceName() {
                return authServiceFactory.getServiceName();
            }

            @Override
            public void configure(ServicePoolBuilder<Databus> servicePoolBuilder) {
                servicePoolBuilder.withPartitionFilter(new ConsistentHashPartitionFilter())
                        .withPartitionContextAnnotationsFrom(DatabusAuthenticatorProxy.class);
            }

            @Override
            public Databus create(ServiceEndPoint endPoint) {
                AuthDatabus authDatabus = authServiceFactory.create(endPoint);
                return new DatabusAuthenticatorProxy(authDatabus, apiKey);
            }

            @Override
            public void destroy(ServiceEndPoint endPoint, Databus service) {
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
