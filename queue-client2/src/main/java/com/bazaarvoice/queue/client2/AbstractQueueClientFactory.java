package com.bazaarvoice.queue.client2;

import com.bazaarvoice.emodb.client2.EmoClient;
import com.bazaarvoice.emodb.common.jersey2.RetryPolicy;
import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.queue.api.QueueService;

import java.net.URI;
import java.util.Objects;

/**
 * Abstract parent class for queue clients.  Subclasses are expected to create and configure an
 * {@link EmoClient} and then supply it via the constructor.
 */
abstract public class AbstractQueueClientFactory {

    private final EmoClient _client;
    private URI _endPoint;

    protected AbstractQueueClientFactory(EmoClient client, URI endPoint) {
        _client = Objects.requireNonNull(client);
        _endPoint = Objects.requireNonNull(endPoint);
    }

    public QueueService usingCredentials(String apiKey) {
        AuthQueueService authQueueService = new QueueClient(_endPoint, _client, RetryPolicy.createDefault());
        return new QueueServiceAuthenticatorProxy(authQueueService, apiKey);
    }
}
