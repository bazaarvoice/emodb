package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;

public class DataStoreClientFactory extends AbstractDataStoreClientFactory {

    /**
     * Connects to the System of Record using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static DataStoreClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        client.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
        return new DataStoreClientFactory(clusterName, client);
    }

    private DataStoreClientFactory(String clusterName, Client jerseyClient) {
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
