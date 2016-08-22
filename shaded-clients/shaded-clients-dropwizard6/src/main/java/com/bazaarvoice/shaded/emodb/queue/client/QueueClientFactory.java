package com.bazaarvoice.shaded.emodb.queue.client;

import com.bazaarvoice.emodb.queue.client.AbstractQueueClientFactory;
import com.bazaarvoice.shaded.emodb.dropwizard6.DropWizard6EmoClient;
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
import com.yammer.dropwizard.client.HttpClientBuilder;
import com.yammer.dropwizard.client.HttpClientConfiguration;
import com.yammer.dropwizard.util.Duration;
import org.apache.http.client.HttpClient;

public class QueueClientFactory extends AbstractQueueClientFactory {

    public static QueueClientFactory forCluster(String clusterName) {
        HttpClientConfiguration httpClientConfiguration = new HttpClientConfiguration();
        httpClientConfiguration.setKeepAlive(Duration.seconds(1));
        return new QueueClientFactory(clusterName, createDefaultJerseyClient(httpClientConfiguration));
    }

    /**
     * Connects to the System of Record using the specified Jersey client.  If you're using Dropwizard, use this
     * factory method and pass the Dropwizard-constructed Jersey client.
     */
    public static QueueClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new QueueClientFactory(clusterName, client);
    }

    public static QueueClientFactory forClusterAndHttpConfiguration(String clusterName, HttpClientConfiguration configuration) {
        return new QueueClientFactory(clusterName, createDefaultJerseyClient(configuration));
    }

    private QueueClientFactory(String clusterName, Client jerseyClient) {
        super(clusterName, new DropWizard6EmoClient(jerseyClient));
    }

    private static ApacheHttpClient4 createDefaultJerseyClient(HttpClientConfiguration configuration) {
        HttpClient httpClient = new HttpClientBuilder().using(configuration).build();
        ApacheHttpClient4Handler handler = new ApacheHttpClient4Handler(httpClient, null, true);
        ApacheHttpClient4Config config = new DefaultApacheHttpClient4Config();
        // For shading reasons we can't add a Jackson JSON message body provider.  However, our client implementation will
        // handle wrapping all request and response entities using the shaded Jackson so this shouldn't matter.
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
