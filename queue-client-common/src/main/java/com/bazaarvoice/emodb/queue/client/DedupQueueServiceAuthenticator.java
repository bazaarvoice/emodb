package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.auth.InvalidCredentialException;
import com.bazaarvoice.emodb.auth.proxy.CachingAuthenticatingProxy;
import com.bazaarvoice.emodb.queue.api.AuthDedupQueueService;
import com.bazaarvoice.emodb.queue.api.DedupQueueService;

public class DedupQueueServiceAuthenticator extends CachingAuthenticatingProxy<DedupQueueService, String> {

    private final AuthDedupQueueService _authDedupQueueService;

    public static DedupQueueServiceAuthenticator proxied(AuthDedupQueueService authDedupQueueService) {
        return new DedupQueueServiceAuthenticator(authDedupQueueService);
    }

    private DedupQueueServiceAuthenticator(AuthDedupQueueService authDedupQueueService) {
        _authDedupQueueService = authDedupQueueService;
    }

    @Override
    protected String validateCredentials(String apiKey) throws InvalidCredentialException {
        if (apiKey.isEmpty()) {
            throw new InvalidCredentialException("API key cannot be empty");
        }
        return apiKey;
    }

    @Override
    protected DedupQueueService createInstanceWithCredentials(String apiKey) {
        return new DedupQueueServiceAuthenticatorProxy(_authDedupQueueService, apiKey);
    }
}
