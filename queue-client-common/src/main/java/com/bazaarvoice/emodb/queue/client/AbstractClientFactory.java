package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.common.dropwizard.discovery.Payload;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.google.common.net.HttpHeaders;

import java.net.URI;

abstract public class AbstractClientFactory<S> implements MultiThreadedServiceFactory<S> {
    private final EmoClient _client;

    protected AbstractClientFactory(EmoClient client) {
        _client = client;
    }

    protected abstract S newClient(URI serviceUrl, boolean partitionSafe, EmoClient client);

    @Override
    public S create(ServiceEndPoint endPoint) {
        Payload payload = Payload.valueOf(endPoint.getPayload());
        boolean partitionSafe = Boolean.FALSE.equals(payload.getExtensions().get("proxy"));
        return newClient(payload.getServiceUrl(), partitionSafe, _client);
    }

    @Override
    public void destroy(ServiceEndPoint endPoint, S service) {
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
}
