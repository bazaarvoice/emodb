package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.common.dropwizard.discovery.Payload;
import com.bazaarvoice.emodb.common.dropwizard.discovery.ServiceNames;
import com.bazaarvoice.emodb.common.json.JsonStreamingEOFException;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.google.common.net.HttpHeaders;

import java.net.URI;

/**
 * Abstract parent base class for data store clients.
 */
abstract public class AbstractDataStoreClientFactoryBase<T> implements MultiThreadedServiceFactory<T> {

    private final String _clusterName;
    protected final EmoClient _client;

    protected AbstractDataStoreClientFactoryBase(String clusterName, EmoClient client) {
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
    public void configure(ServicePoolBuilder<T> servicePoolBuilder) {
        // Defaults are ok
    }

    @Override
    public abstract T create(ServiceEndPoint endPoint);

    @Override
    public void destroy(ServiceEndPoint endPoint, T service) {
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
}
