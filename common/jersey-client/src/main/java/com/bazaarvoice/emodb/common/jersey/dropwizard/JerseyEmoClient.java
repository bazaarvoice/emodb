package com.bazaarvoice.emodb.common.jersey.dropwizard;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoResource;
import com.sun.jersey.api.client.Client;

import java.net.URI;

import static java.util.Objects.requireNonNull;


/**
 * EmoClient implementation that uses a Jersey client.
 */
public class JerseyEmoClient implements EmoClient {

    private final Client _client;

    public JerseyEmoClient(Client client) {
        _client = requireNonNull(client, "client");
    }

    @Override
    public EmoResource resource(URI uri) {
        return new JerseyEmoResource(_client.resource(uri));
    }
}
