package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.RetryPolicy;
import com.bazaarvoice.emodb.queue.api.AuthDedupQueueService;
import com.bazaarvoice.emodb.queue.api.DedupQueueService;

import java.net.URI;
import java.util.Objects;

public class AbstractDedupQueueClientFactory {

    private final EmoClient _client;
    private final URI _endPoint;

    protected AbstractDedupQueueClientFactory(EmoClient client, URI endPoint) {
        _client = Objects.requireNonNull(client);
        _endPoint = Objects.requireNonNull(endPoint);
    }

    public DedupQueueService usingCredentials(String apiKey) {
        AuthDedupQueueService dedupQueueService = new DedupQueueClient(_endPoint, _client, RetryPolicy.createDefault());
        return new DedupQueueServiceAuthenticatorProxy(dedupQueueService, apiKey);
    }
}
