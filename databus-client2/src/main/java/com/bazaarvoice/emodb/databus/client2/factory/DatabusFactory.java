package com.bazaarvoice.emodb.databus.client2.factory;


import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.common.jersey2.Jersey2EmoClient;
import com.bazaarvoice.emodb.databus.client2.client.DatabusClient;
import com.bazaarvoice.emodb.databus.client2.discovery.DatabusDiscovery;
import com.google.common.base.Throwables;
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
 * Factory for creating {@link DatabusClient} clients.
 */
public class DatabusFactory implements Serializable {

    private final Logger _log = LoggerFactory.getLogger(DatabusFactory.class);

    private final DatabusDiscovery _databusDiscovery;
    private final String _apiKey;

    public DatabusFactory(DatabusDiscovery discovery, String apiKey) {
        _databusDiscovery = requireNonNull(discovery, "Databus discovery is required");
        _apiKey = requireNonNull(apiKey, "API key is required");
    }

    public DatabusClient create(JerseyClient client) {
        Service service = _databusDiscovery.startAsync();

        try {
            service.awaitRunning(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            _log.error("Databus discovery did not start in a reasonable time");
            throw Throwables.propagate(e);
        } catch (Exception e) {
            _log.error("Databus discovery startup failed", e);
        }

        return new DatabusClient(_databusDiscovery, new Jersey2EmoClient(client), _apiKey);
    }
}
