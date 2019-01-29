package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.codahale.metrics.MetricRegistry;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.util.Duration;
import javax.ws.rs.client.Client;

public class QueueClientFactory extends AbstractQueueClientFactory {

    public static QueueClientFactory forCluster(String clusterName, MetricRegistry metricRegistry) {
        JerseyClientConfiguration jerseyClientConfiguration = new JerseyClientConfiguration();
        jerseyClientConfiguration.setKeepAlive(Duration.seconds(1));
        return new QueueClientFactory(clusterName, createDefaultJerseyClient(jerseyClientConfiguration, getServiceName(clusterName), metricRegistry));
    }

    /**
     * Connects to the QueueService using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static QueueClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new QueueClientFactory(clusterName, client);
    }

    public static QueueClientFactory forClusterAndHttpConfiguration(String clusterName, JerseyClientConfiguration configuration, MetricRegistry metricRegistry) {
        return new QueueClientFactory(clusterName, createDefaultJerseyClient(configuration, getServiceName(clusterName), metricRegistry));
    }

    private QueueClientFactory(String clusterName, Client jerseyClient) {
        super(clusterName, new JerseyEmoClient(jerseyClient));
    }

    private static Client createDefaultJerseyClient(JerseyClientConfiguration configuration, String serviceName, MetricRegistry metricRegistry) {
        return new JerseyClientBuilder(metricRegistry).using(configuration).build(serviceName);
    }

}