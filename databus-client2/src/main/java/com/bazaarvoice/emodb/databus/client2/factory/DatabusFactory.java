package com.bazaarvoice.emodb.databus.client2.factory;

import com.bazaarvoice.emodb.auth.InvalidCredentialException;
import com.bazaarvoice.emodb.auth.util.CredentialEncrypter;
import com.bazaarvoice.emodb.client2.EmoClient;
import com.bazaarvoice.emodb.common.jersey2.RetryPolicy;
import com.bazaarvoice.emodb.common.jersey2.Jersey2EmoClient;
import com.bazaarvoice.emodb.databus.client2.client.DatabusClient;
import com.bazaarvoice.emodb.databus.client2.discovery.EmoServiceDiscovery;
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

    private final EmoServiceDiscovery _emoServiceDiscovery;
    private final String _apiKey;
    private final EmoClient _emoClient;

    public DatabusFactory(EmoServiceDiscovery emoServiceDiscovery, String apiKey, JerseyClient client) {
        _emoServiceDiscovery = requireNonNull(emoServiceDiscovery, "Service discovery is required");
        _emoClient = new Jersey2EmoClient(requireNonNull(client, "Client is required"));
        _apiKey = requireNonNull(apiKey, "API key is required");

        if (CredentialEncrypter.isPotentiallyEncryptedString(apiKey)) {
            throw new InvalidCredentialException("API Key is encrypted, please decrypt it");
        }
    }

    public DatabusClient create() {
        if (_emoServiceDiscovery instanceof ZKEmoServiceDiscovery) {
            Service service = ((ZKEmoServiceDiscovery) _emoServiceDiscovery).startAsync();

            try {
                service.awaitRunning(30, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                _log.error("Databus discovery did not start in a reasonable time");
                throw Throwables.propagate(e);
            } catch (Exception e) {
                _log.error("Databus discovery startup failed", e);
            }
        }
        return new DatabusClient(_emoServiceDiscovery, _emoClient, _apiKey, RetryPolicy.createDefault());
    }
}
