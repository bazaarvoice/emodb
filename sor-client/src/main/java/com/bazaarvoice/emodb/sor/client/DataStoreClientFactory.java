package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.common.jersey.dropwizard.JerseyEmoClient;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.UniformInterfaceException;
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

public class DataStoreClientFactory extends AbstractDataStoreClientFactory {

    private static ValidatorFactory _validatorFactory = Validation.buildDefaultValidatorFactory();

    public static DataStoreClientFactory forCluster(String clusterName, MetricRegistry metricRegistry) {
        HttpClientConfiguration httpClientConfiguration = new HttpClientConfiguration();
        httpClientConfiguration.setKeepAlive(Duration.seconds(1));
        return new DataStoreClientFactory(clusterName, createDefaultJerseyClient(httpClientConfiguration, getServiceName(clusterName), metricRegistry));
    }

    /**
     * Connects to the System of Record using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static DataStoreClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new DataStoreClientFactory(clusterName, client);
    }

    public static DataStoreClientFactory forClusterAndHttpConfiguration(String clusterName, HttpClientConfiguration configuration, MetricRegistry metricRegistry) {
        return new DataStoreClientFactory(clusterName, createDefaultJerseyClient(configuration, getServiceName(clusterName), metricRegistry));
    }

    private DataStoreClientFactory(String clusterName, Client jerseyClient) {
        super(clusterName, new JerseyEmoClient(jerseyClient));
    }

    private static ApacheHttpClient4 createDefaultJerseyClient(HttpClientConfiguration configuration, String serviceName, MetricRegistry metricRegistry) {
        HttpClient httpClient = new HttpClientBuilder(metricRegistry).using(configuration).build(serviceName);
        ApacheHttpClient4Handler handler = new ApacheHttpClient4Handler(httpClient, null, true);
        ApacheHttpClient4Config config = new DefaultApacheHttpClient4Config();
        config.getSingletons().add(new JacksonMessageBodyProvider(Jackson.newObjectMapper(), _validatorFactory.getValidator()));
        return new ApacheHttpClient4(handler, config);
    }

    @Override
    public boolean isRetriableException(Exception e) {
        return super.isRetriableException(e) ||
                (e instanceof UniformInterfaceException &&
                ((UniformInterfaceException) e).getResponse().getStatus() >= 500) ||
                Iterables.any(Throwables.getCausalChain(e), Predicates.instanceOf(ClientHandlerException.class));
    }
}
