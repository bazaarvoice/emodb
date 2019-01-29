package com.bazaarvoice.emodb.blob.client;

import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.util.Duration;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;

import java.util.concurrent.ScheduledExecutorService;

public class BlobStoreClientFactory extends AbstractBlobStoreClientFactory {

    public static BlobStoreClientFactory forCluster(String clusterName, MetricRegistry metricRegistry) {
        JerseyClientConfiguration jerseyClientConfiguration = new JerseyClientConfiguration();
        jerseyClientConfiguration.setKeepAlive(Duration.seconds(1));
        return new BlobStoreClientFactory(clusterName, createDefaultJerseyClient(jerseyClientConfiguration, metricRegistry, getServiceName(clusterName)));
    }

    /**
     * Connects to the Blob Store using the specified Jersey client.  If you're using Dropwizard, use this
     * constructor and pass the Dropwizard-constructed Jersey client.
     */
    public static BlobStoreClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new BlobStoreClientFactory(clusterName, client);
    }

    public static BlobStoreClientFactory forClusterAndHttpConfiguration(String clusterName, JerseyClientConfiguration configuration, MetricRegistry metricRegistry) {
        return new BlobStoreClientFactory(clusterName, createDefaultJerseyClient(configuration, metricRegistry, getServiceName(clusterName)));
    }

    public BlobStoreClientFactory withConnectionManagementService(ScheduledExecutorService service) {
        setConnectionManagementService(service);
        return this;
    }

    private BlobStoreClientFactory(String clusterName, Client jerseyClient) {
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