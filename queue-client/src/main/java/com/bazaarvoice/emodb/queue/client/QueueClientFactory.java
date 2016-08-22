package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.codahale.metrics.MetricRegistry;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.client.apache4.ApacheHttpClient4;
import com.sun.jersey.client.apache4.ApacheHttpClient4Handler;
import com.sun.jersey.client.apache4.config.ApacheHttpClient4Config;
import com.sun.jersey.client.apache4.config.DefaultApacheHttpClient4Config;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.client.HttpClientConfiguration;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.jersey.jackson.JacksonMessageBodyProvider;
import io.dropwizard.util.Duration;
import org.apache.http.client.HttpClient;

import javax.validation.Validation;
import javax.validation.ValidatorFactory;

public class QueueClientFactory extends AbstractQueueClientFactory {

    private static ValidatorFactory _validatorFactory = Validation.buildDefaultValidatorFactory();

    public static QueueClientFactory forCluster(String clusterName, MetricRegistry metricRegistry) {
        HttpClientConfiguration httpClientConfiguration = new HttpClientConfiguration();
        httpClientConfiguration.setKeepAlive(Duration.seconds(1));
        return new QueueClientFactory(clusterName, createDefaultJerseyClient(httpClientConfiguration, getServiceName(clusterName), metricRegistry));
    }

    /**
     * Connects to the QueueService using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static QueueClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new QueueClientFactory(clusterName, client);
    }

    public static QueueClientFactory forClusterAndHttpConfiguration(String clusterName, HttpClientConfiguration configuration, MetricRegistry metricRegistry) {
        return new QueueClientFactory(clusterName, createDefaultJerseyClient(configuration, getServiceName(clusterName), metricRegistry));
    }

    private QueueClientFactory(String clusterName, Client jerseyClient) {
        super(clusterName, new JerseyEmoClient(jerseyClient));
    }

    static ApacheHttpClient4 createDefaultJerseyClient(HttpClientConfiguration configuration, String serviceName, MetricRegistry metricRegistry) {
        HttpClient httpClient = new HttpClientBuilder(metricRegistry).using(configuration).build(serviceName);
        ApacheHttpClient4Handler handler = new ApacheHttpClient4Handler(httpClient, null, true);
        ApacheHttpClient4Config config = new DefaultApacheHttpClient4Config();
        config.getSingletons().add(new JacksonMessageBodyProvider(Jackson.newObjectMapper(), _validatorFactory.getValidator()));
        return new ApacheHttpClient4(handler, config);
    }

}
