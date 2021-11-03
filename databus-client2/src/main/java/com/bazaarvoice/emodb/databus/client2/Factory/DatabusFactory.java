package com.bazaarvoice.emodb.databus.client2.Factory;


import com.bazaarvoice.emodb.databus.client2.client.Databus;
import com.bazaarvoice.emodb.databus.client2.discovery.DatabusDiscovery;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.JerseyClient;
import org.glassfish.jersey.client.JerseyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * Factory for creating {@link Databus} clients.
 */
public class DatabusFactory implements Serializable {

    private final Logger _log = LoggerFactory.getLogger(DatabusFactory.class);

    private final DatabusDiscovery.Builder _databusDiscoveryBuilder;
    private final String _apiKey;

    public DatabusFactory(DatabusDiscovery.Builder databusDiscoveryBuilder, String apiKey) {
        _databusDiscoveryBuilder = requireNonNull(databusDiscoveryBuilder, "Databus discovery builder is required");
        _apiKey = requireNonNull(apiKey, "API key is required");
    }

    public Databus create() {
        DatabusDiscovery databusDiscovery = _databusDiscoveryBuilder.build();
        ListenableFuture<Service.State> future = databusDiscovery.startAsync();

        try {
            future.get(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            _log.error("Databus discovery did not start in a reasonable time");
            throw Throwables.propagate(e);
        } catch (Exception e) {
            _log.error("Databus discovery startup failed", e);
        }

        JerseyClient client = JerseyClientBuilder.createClient(new ClientConfig()
                .property(ClientProperties.CONNECT_TIMEOUT, (int) Duration.ofSeconds(5).toMillis())
                .property(ClientProperties.READ_TIMEOUT, (int) Duration.ofSeconds(60).toMillis()));

        return new Databus(databusDiscovery, client, _apiKey);
    }
}
