package com.bazaarvoice.queue.client2;

import com.bazaarvoice.emodb.client2.EmoClient;
import com.bazaarvoice.emodb.common.jersey2.RetryPolicy;
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
