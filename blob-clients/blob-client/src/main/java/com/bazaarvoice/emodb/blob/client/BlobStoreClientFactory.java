package com.bazaarvoice.emodb.blob.client;

import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import java.util.concurrent.ScheduledExecutorService;

public class BlobStoreClientFactory extends AbstractBlobStoreClientFactory {


    /**
     * Connects to the Blob Store using the specified Jersey client.  If you're using Dropwizard, use this
     * constructor and pass the Dropwizard-constructed Jersey client.
     */
    public static BlobStoreClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        client.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
        return new BlobStoreClientFactory(clusterName, client);
    }

    public BlobStoreClientFactory withConnectionManagementService(ScheduledExecutorService service) {
        setConnectionManagementService(service);
        return this;
    }

    private BlobStoreClientFactory(String clusterName, Client jerseyClient) {
        super(clusterName, new JerseyEmoClient(jerseyClient));
    }

    @Override
    public boolean isRetriableException(Exception e) {
        return super.isRetriableException(e) ||
                (e instanceof WebApplicationException &&
                        ((WebApplicationException) e).getResponse().getStatus() >= 500) ||
                Iterables.any(Throwables.getCausalChain(e), Predicates.instanceOf(ProcessingException.class));
    }
}
