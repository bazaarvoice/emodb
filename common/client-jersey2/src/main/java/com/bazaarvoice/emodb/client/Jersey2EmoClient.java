package com.bazaarvoice.emodb.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoResource;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import javax.ws.rs.client.Client;
import java.net.URI;

import static java.util.Objects.requireNonNull;


/**
 * EmoClient implementation that uses a Jersey client.
 */
public class Jersey2EmoClient implements EmoClient {

    private final Client _client;

    public Jersey2EmoClient(final Client client) {
        _client = requireNonNull(client, "client");
        if (!_client.getConfiguration().isRegistered(JacksonJsonProvider.class)) {
            _client.register(JacksonJsonProvider.class);
        }
    }

    @Override
    public EmoResource resource(URI uri) {
        return new Jersey2EmoResource(_client.target(uri));
    }
}
