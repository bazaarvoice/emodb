package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.common.dropwizard.discovery.Payload;
import com.bazaarvoice.emodb.common.dropwizard.discovery.ServiceNames;
import com.bazaarvoice.emodb.common.json.JsonStreamingEOFException;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.google.common.net.HttpHeaders;

import java.net.URI;

/**
 * Abstract parent class for data store clients.  Subclasses are expected to create and configure an
 * {@link EmoClient} and then supply it via the constructor.
 */
abstract public class AbstractDataStoreClientFactory implements MultiThreadedServiceFactory<AuthDataStore> {

    private final String _clusterName;
    private final EmoClient _client;

    protected AbstractDataStoreClientFactory(String clusterName, EmoClient client) {
        _clusterName = clusterName;
        _client = client;
    }

    @Override
    public String getServiceName() {
        return getServiceName(_clusterName);
    }

    protected static String getServiceName(String clusterName) {
        return ServiceNames.forNamespaceAndBaseServiceName(clusterName, DataStoreClient.BASE_SERVICE_NAME);
    }

    @Override
    public void configure(ServicePoolBuilder<AuthDataStore> servicePoolBuilder) {
        // Defaults are ok
    }

    @Override
    public AuthDataStore create(ServiceEndPoint endPoint) {
        Payload payload = Payload.valueOf(endPoint.getPayload());
        return new DataStoreClient(payload.getServiceUrl(), _client);
    }

    @Override
    public void destroy(ServiceEndPoint endPoint, AuthDataStore service) {
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

    public MultiThreadedServiceFactory<DataStore> usingCredentials(final String apiKey) {
        final AbstractDataStoreClientFactory authServiceFactory = this;

        return new MultiThreadedServiceFactory<DataStore>() {
            @Override
            public String getServiceName() {
                return authServiceFactory.getServiceName();
            }

            @Override
            public void configure(ServicePoolBuilder<DataStore> servicePoolBuilder) {
                // Defaults are ok
            }

            @Override
            public DataStore create(ServiceEndPoint endPoint) {
                AuthDataStore authDataStore = authServiceFactory.create(endPoint);
                return new DataStoreAuthenticatorProxy(authDataStore, apiKey);
            }

            @Override
            public void destroy(ServiceEndPoint endPoint, DataStore service) {
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
