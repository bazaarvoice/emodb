package com.bazaarvoice.emodb.common.jersey.dropwizard;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoResource;

import javax.ws.rs.client.Client;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * EmoClient implementation that uses a Jersey client.
 */
public class JerseyEmoClient implements EmoClient {

    private final Client _client;

    public JerseyEmoClient(Client client) {
        _client = checkNotNull(client, "client");
    }

    @Override
    public EmoResource resource(URI uri) {
        return new JerseyEmoResource(_client.target(uri));
    }
}
