package com.bazaarvoice.emodb.common.jersey.dropwizard;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoResource;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import javax.ws.rs.client.Client;
import java.net.URI;

import static java.util.Objects.requireNonNull;


/**
 * EmoClient implementation that uses a Jersey client.
 */
public class JerseyEmoClient implements EmoClient {

    private final Client _client;

    public JerseyEmoClient( final Client client) {
        _client = requireNonNull(client, "client");
        if(!_client.getConfiguration().isRegistered(JacksonJsonProvider.class)) {
            _client.register(JacksonJsonProvider.class);
        }
    }

    @Override
    public EmoResource resource(URI uri) {
        return new JerseyEmoResource(_client.target(uri));
    }
}
