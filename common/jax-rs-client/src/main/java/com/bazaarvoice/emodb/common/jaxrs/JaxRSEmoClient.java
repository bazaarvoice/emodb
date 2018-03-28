package com.bazaarvoice.emodb.common.jaxrs;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoResource;

import javax.ws.rs.client.Client;
import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * EmoClient implementation that uses a Jersey client.
 */
public class JaxRSEmoClient implements EmoClient {

    private final Client _client;

    public JaxRSEmoClient(Client client) {
        _client = checkNotNull(client, "client");
    }

    @Override
    public EmoResource resource(URI uri) {
        return new JaxRSEmoResource(_client.target(uri));
    }
}
