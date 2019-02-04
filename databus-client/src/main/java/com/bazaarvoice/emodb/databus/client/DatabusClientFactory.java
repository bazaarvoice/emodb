package com.bazaarvoice.emodb.databus.client;

import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.util.Duration;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;

public class DatabusClientFactory extends AbstractDatabusClientFactory {

    /**
     * Connects to the Databus using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static DatabusClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new DatabusClientFactory(clusterName, client);
    }

    protected DatabusClientFactory(String clusterName, Client jerseyClient) {
        super(clusterName, new JerseyEmoClient(jerseyClient));
    }

    @Override
    public boolean isRetriableException(Exception e) {
        // TODO: explore if the removal of ClientHandlerException is acceptable
        return super.isRetriableException(e) ||
                (e instanceof WebApplicationException &&
                        ((WebApplicationException) e).getResponse().getStatus() >= 500);
    }
}