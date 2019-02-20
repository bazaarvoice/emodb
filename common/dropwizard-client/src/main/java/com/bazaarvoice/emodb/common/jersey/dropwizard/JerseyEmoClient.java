package com.bazaarvoice.emodb.common.jersey.dropwizard;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoResource;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import java.net.URI;
import javax.ws.rs.client.Client;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * EmoClient implementation that uses a Jersey client.
 */
public class JerseyEmoClient implements EmoClient {

    private final Client _client;

    public JerseyEmoClient(Client client) {
        _client = checkNotNull(client, "client");
        _client.register(JacksonJsonProvider.class);
    }

    @Override
    public EmoResource resource(URI uri) {
        return new JerseyEmoResource(_client.target(uri));
    }
}
