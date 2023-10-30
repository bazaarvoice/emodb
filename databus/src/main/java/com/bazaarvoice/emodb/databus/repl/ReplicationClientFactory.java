package com.bazaarvoice.emodb.databus.repl;

import com.bazaarvoice.emodb.common.dropwizard.discovery.Payload;
import com.bazaarvoice.ostrich.MultiThreadedServiceFactory;
import com.bazaarvoice.ostrich.ServiceEndPoint;
import com.bazaarvoice.ostrich.pool.ServicePoolBuilder;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.net.HttpHeaders;
import javax.ws.rs.client.Client;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;

import java.net.URI;
import java.util.Objects;

/**
 * SOA factory for Jersey clients for downloading databus events from a remote data center.
 */
public class ReplicationClientFactory implements MultiThreadedServiceFactory<ReplicationSource> {
    private final Client _jerseyClient;
    private final String _apiKey;

    public ReplicationClientFactory(Client jerseyClient) {
        this(jerseyClient, null);
    }

    public ReplicationClientFactory(Client jerseyClient, String apiKey) {
        _jerseyClient = jerseyClient;
        _apiKey = apiKey;
    }

    /**
     * Creates a view of this instance using the given API Key and sharing the same underlying resources.
     * Note that this method may return a new instance so the caller must use the returned value.
     */
    public ReplicationClientFactory usingApiKey(String apiKey) {
        if (Objects.equals(_apiKey, apiKey)) {
            return this;
        }
        return new ReplicationClientFactory(_jerseyClient, apiKey);
    }

    @Override
    public String getServiceName() {
        return "emodb-busrepl-1";
    }

    @Override
    public void configure(ServicePoolBuilder<ReplicationSource> servicePoolBuilder) {
        // Nothing to do
    }

    @Override
    public ReplicationSource create(ServiceEndPoint endPoint) {
        Payload payload = Payload.valueOf(endPoint.getPayload());
        return new ReplicationClient(payload.getServiceUrl(), _jerseyClient, _apiKey);
    }

    @Override
    public void destroy(ServiceEndPoint endPoint, ReplicationSource service) {
        // Nothing to do
    }

    @Override
    public boolean isRetriableException(Exception e) {
        return (e instanceof WebApplicationException &&
                ((WebApplicationException) e).getResponse().getStatus() >= 500) ||
                Iterables.any(Throwables.getCausalChain(e), Predicates.instanceOf(ProcessingException.class));
    }

    @Override
    public boolean isHealthy(ServiceEndPoint endPoint) {
        URI adminUrl = Payload.valueOf(endPoint.getPayload()).getAdminUrl();
        return _jerseyClient.target(adminUrl).path("/healthcheck").request()
                .header(HttpHeaders.CONNECTION, "close")
                .head().getStatus() == 200;
    }
}
