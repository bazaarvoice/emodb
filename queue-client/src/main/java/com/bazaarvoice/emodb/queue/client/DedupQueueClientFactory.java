package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.util.Duration;
import javax.ws.rs.client.Client;

public class DedupQueueClientFactory extends AbstractDedupQueueClientFactory {

    public static DedupQueueClientFactory forCluster(String clusterName, MetricRegistry metricRegistry) {
        JerseyClientConfiguration jerseyClientConfiguration = new JerseyClientConfiguration();
        jerseyClientConfiguration.setKeepAlive(Duration.seconds(1));
        return new DedupQueueClientFactory(clusterName, createDefaultJerseyClient(jerseyClientConfiguration, getServiceName(clusterName), metricRegistry));
    }

    /**
     * Connects to the DedupQueueService using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static DedupQueueClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new DedupQueueClientFactory(clusterName, client);
    }

    public static DedupQueueClientFactory forClusterAndHttpConfiguration(String clusterName, JerseyClientConfiguration configuration, MetricRegistry metricRegistry) {
        return new DedupQueueClientFactory(clusterName, createDefaultJerseyClient(configuration, getServiceName(clusterName), metricRegistry));
    }

    private DedupQueueClientFactory(String clusterName, Client jerseyClient) {
        super(clusterName, new JerseyEmoClient(jerseyClient));
    }

    private static Client createDefaultJerseyClient(JerseyClientConfiguration configuration, String serviceName, MetricRegistry metricRegistry) {
        return new JerseyClientBuilder(metricRegistry).using(configuration).build(serviceName);
    }
}