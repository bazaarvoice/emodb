package com.bazaarvoice.shaded.emodb.dropwizard6;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoResource;
import com.sun.jersey.api.client.Client;

import java.net.URI;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * EmoClient implementation that uses a Jersey client version compatible with Dropwizard 6.
 */
public class DropWizard6EmoClient implements EmoClient {

    private final Client _client;

    public DropWizard6EmoClient(Client client) {
        _client = checkNotNull(client, "client");
    }

    @Override
    public EmoResource resource(URI uri) {
        return new DropWizard6EmoResource(_client.resource(uri));
    }
}
