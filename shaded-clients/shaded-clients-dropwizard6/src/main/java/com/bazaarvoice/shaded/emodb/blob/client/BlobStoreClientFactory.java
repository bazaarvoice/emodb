package com.bazaarvoice.shaded.emodb.blob.client;

import com.bazaarvoice.emodb.blob.client.AbstractBlobStoreClientFactory;
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

import java.util.concurrent.ScheduledExecutorService;

public class BlobStoreClientFactory extends AbstractBlobStoreClientFactory {

    public static BlobStoreClientFactory forCluster(String clusterName) {
        HttpClientConfiguration httpClientConfiguration = new HttpClientConfiguration();
        httpClientConfiguration.setKeepAlive(Duration.seconds(1));
        return new BlobStoreClientFactory(clusterName, createDefaultJerseyClient(httpClientConfiguration));
    }

    /**
     * Connects to the Blob Store using the specified Jersey client.  If you're using Dropwizard, use this
     * constructor and pass the Dropwizard-constructed Jersey client.
     */
    public static BlobStoreClientFactory forClusterAndHttpClient(String clusterName, Client client) {
        return new BlobStoreClientFactory(clusterName, client);
    }

    public static BlobStoreClientFactory forClusterAndHttpConfiguration(String clusterName, HttpClientConfiguration configuration) {
        return new BlobStoreClientFactory(clusterName, createDefaultJerseyClient(configuration));
    }

    public BlobStoreClientFactory withConnectionManagementService(ScheduledExecutorService service) {
        setConnectionManagementService(service);
        return this;
    }

    private BlobStoreClientFactory(String clusterName, Client jerseyClient) {
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