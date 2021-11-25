package com.bazaarvoice.emodb.common.jersey2;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoResource;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import javax.ws.rs.client.Client;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * EmoClient implementation that uses a Jersey client.
 */
public class Jersey2EmoClient implements EmoClient {

    private final Client _client;

    public Jersey2EmoClient(final Client client) {
        _client = checkNotNull(client, "client");
        if(!_client.getConfiguration().isRegistered(JacksonJsonProvider.class)) {
            _client.register(JacksonJsonProvider.class);
        }
    }

    @Override
    public EmoResource resource(URI uri) {
        return new Jersey2EmoResource(_client.target(uri));
    }
}
