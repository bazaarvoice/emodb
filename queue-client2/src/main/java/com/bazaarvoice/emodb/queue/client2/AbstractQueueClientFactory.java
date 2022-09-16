package com.bazaarvoice.emodb.queue.client2;

import com.bazaarvoice.emodb.auth.InvalidCredentialException;
import com.bazaarvoice.emodb.auth.util.CredentialEncrypter;
import com.bazaarvoice.emodb.client2.EmoClient;
import com.bazaarvoice.emodb.common.jersey2.RetryPolicy;
import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.queue.api.QueueService;

import java.net.URI;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Abstract parent class for queue clients.  Subclasses are expected to create and configure an
 * {@link EmoClient} and then supply it via the constructor.
 */
abstract public class AbstractQueueClientFactory {

    private final EmoClient _client;
    private final URI _endPoint;

    protected AbstractQueueClientFactory(EmoClient client, URI endPoint) {
        _client = Objects.requireNonNull(client);
        _endPoint = Objects.requireNonNull(endPoint);
    }

    public QueueService usingCredentials(String apiKey) {
        AuthQueueService authQueueService = new QueueClient(_endPoint, _client, RetryPolicy.createDefault());
        return new QueueServiceAuthenticatorProxy(authQueueService, validateApiKey(apiKey));
    }

    private static String validateApiKey(String apiKey) throws InvalidCredentialException {
        requireNonNull(apiKey, "API key is required");
        if (apiKey.isEmpty()) {
            throw new InvalidCredentialException("API key cannot be empty");
        }
        if (CredentialEncrypter.isPotentiallyEncryptedString(apiKey)) {
            throw new InvalidCredentialException("API Key is encrypted, please decrypt it");
        }
        return apiKey;
    }
}
