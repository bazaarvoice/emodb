package com.bazaarvoice.emodb.blob.client;

import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.RetryPolicy;

import java.io.Serializable;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract parent class for blob store clients.  Subclasses are expected to create and configure an
 * {@link EmoClient} and then supply it via the constructor.
 */
abstract public class AbstractBlobStoreClientFactory implements Serializable {

    private final EmoClient _client;
    private final URI _endPoint;
    private ScheduledExecutorService _connectionManagementService;

    protected AbstractBlobStoreClientFactory(EmoClient client, URI endPoint) {
        _client = Objects.requireNonNull(client);
        _endPoint = Objects.requireNonNull(endPoint);
    }

    public BlobStore usingCredentials(final String apiKey) {
        AuthBlobStore authBlobStore = new BlobStoreClient(_endPoint, _client,
                _connectionManagementService, RetryPolicy.createDefault());
        return new BlobStoreAuthenticatorProxy(authBlobStore, apiKey);
    }

    protected void setConnectionManagementService(ScheduledExecutorService connectionManagementService) {
        _connectionManagementService = connectionManagementService;
    }
}
