package com.bazaarvoice.emodb.databus.client2.factory;

import com.bazaarvoice.emodb.common.jersey2.Jersey2EmoClient;
import com.bazaarvoice.emodb.databus.client2.client.DatabusClient;
import com.bazaarvoice.emodb.databus.client2.discovery.DirectUriEmoServiceDiscovery;
import com.bazaarvoice.emodb.databus.client2.discovery.ZKEmoServiceDiscovery;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service;
import org.glassfish.jersey.client.JerseyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * Factory for creating {@link DatabusClient} clients.
 */
public class DatabusFactory implements Serializable {

    private final Logger _log = LoggerFactory.getLogger(DatabusFactory.class);

    private final ZKEmoServiceDiscovery _zkEmoServiceDiscovery;
    private final DirectUriEmoServiceDiscovery _directUriEmoServiceDiscovery;
    private final String _apiKey;

    public DatabusFactory(ZKEmoServiceDiscovery zkEmoServiceDiscovery, String apiKey) {
        _zkEmoServiceDiscovery = requireNonNull(zkEmoServiceDiscovery,"ZK service discovery is required");
        _apiKey = requireNonNull(apiKey, "API key is required");
        _directUriEmoServiceDiscovery = null;
    }

    public DatabusFactory(DirectUriEmoServiceDiscovery directUriEmoServiceDiscovery, String apiKey) {
        _directUriEmoServiceDiscovery = requireNonNull(directUriEmoServiceDiscovery,"direct uri is required for Emo service discovery");
        _apiKey = requireNonNull(apiKey, "API key is required");
        _zkEmoServiceDiscovery = null;
    }

    public DatabusClient create(JerseyClient client) {
        DatabusClient databusClient = null;
        if(_zkEmoServiceDiscovery != null){
            Service service = _zkEmoServiceDiscovery.startAsync();

            try {
                service.awaitRunning(30, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                _log.error("Databus discovery did not start in a reasonable time");
                throw Throwables.propagate(e);
            } catch (Exception e) {
                _log.error("Databus discovery startup failed", e);
            }
            databusClient = new DatabusClient(_zkEmoServiceDiscovery, new Jersey2EmoClient(client), _apiKey);
        } else{
            databusClient = new DatabusClient(_directUriEmoServiceDiscovery, new Jersey2EmoClient(client), _apiKey);
        }
        return databusClient;
    }
}
