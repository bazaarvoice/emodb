package com.bazaarvoice.emodb.databus.client;

import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.util.Duration;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;

public class DatabusClientFactory extends AbstractDatabusClientFactory {

    public static DatabusClientFactory forCluster(String clusterName, MetricRegistry metricRegistry) {
        JerseyClientConfiguration jerseyClientConfiguration = new JerseyClientConfiguration();
        jerseyClientConfiguration.setKeepAlive(Duration.seconds(1));
        return new DatabusClientFactory(clusterName, createDefaultJerseyClient(jerseyClientConfiguration, metricRegistry, getServiceName(clusterName)));
    }

    /**
     * Connects to the Databus using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static DatabusClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new DatabusClientFactory(clusterName, client);
    }

    public static DatabusClientFactory forClusterAndHttpConfiguration(String clusterName, JerseyClientConfiguration configuration, MetricRegistry metricRegistry) {
        return new DatabusClientFactory(clusterName, createDefaultJerseyClient(configuration, metricRegistry, getServiceName(clusterName)));
    }

    protected DatabusClientFactory(String clusterName, Client jerseyClient) {
        super(clusterName, new JerseyEmoClient(jerseyClient));
    }

    private static Client createDefaultJerseyClient(JerseyClientConfiguration configuration, MetricRegistry metricRegistry, String serviceName) {
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