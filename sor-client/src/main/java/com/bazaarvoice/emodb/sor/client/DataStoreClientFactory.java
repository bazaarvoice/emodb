package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.util.Duration;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;

public class DataStoreClientFactory extends AbstractDataStoreClientFactory {

    public static DataStoreClientFactory forCluster(String clusterName, MetricRegistry metricRegistry) {
        JerseyClientConfiguration jerseyClientConfiguration = new JerseyClientConfiguration();
        jerseyClientConfiguration.setKeepAlive(Duration.seconds(1));
        return new DataStoreClientFactory(clusterName, createDefaultJerseyClient(jerseyClientConfiguration, getServiceName(clusterName), metricRegistry));
    }

    /**
     * Connects to the System of Record using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static DataStoreClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new DataStoreClientFactory(clusterName, client);
    }

    public static DataStoreClientFactory forClusterAndHttpConfiguration(String clusterName, JerseyClientConfiguration configuration, MetricRegistry metricRegistry) {
        return new DataStoreClientFactory(clusterName, createDefaultJerseyClient(configuration, getServiceName(clusterName), metricRegistry));
    }

    private DataStoreClientFactory(String clusterName, Client jerseyClient) {
        super(clusterName, new JerseyEmoClient(jerseyClient));
    }

    private static Client createDefaultJerseyClient(JerseyClientConfiguration configuration, String serviceName, MetricRegistry metricRegistry) {
        return new JerseyClientBuilder(metricRegistry).using(configuration).build(serviceName);
    }

    @Override
    public boolean isRetriableException(Exception e) {
        // TODO: explore if the removal of ClientHandlerException is acceptable
        return super.isRetriableException(e) ||
                (e instanceof WebApplicationException &&
                        ((WebApplicationException) e).getResponse().getStatus() >= 500);
    }
}