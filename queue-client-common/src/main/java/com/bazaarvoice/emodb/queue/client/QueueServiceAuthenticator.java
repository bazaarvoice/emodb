package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.auth.InvalidCredentialException;
import com.bazaarvoice.emodb.auth.proxy.CachingAuthenticatingProxy;
import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.queue.api.QueueService;

public class QueueServiceAuthenticator extends CachingAuthenticatingProxy<QueueService, String> {

    private final AuthQueueService _authQueueService;

    public static QueueServiceAuthenticator proxied(AuthQueueService authQueueService) {
        return new QueueServiceAuthenticator(authQueueService);
    }

    private QueueServiceAuthenticator(AuthQueueService authQueueService) {
        _authQueueService = authQueueService;
    }

    @Override
    protected String validateCredentials(String apiKey) throws InvalidCredentialException {
        if (apiKey.isEmpty()) {
            throw new InvalidCredentialException("API key cannot be empty");
        }
        return apiKey;
    }

    @Override
    protected QueueService createInstanceWithCredentials(String apiKey) {
        return new QueueServiceAuthenticatorProxy(_authQueueService, apiKey);
    }
}
