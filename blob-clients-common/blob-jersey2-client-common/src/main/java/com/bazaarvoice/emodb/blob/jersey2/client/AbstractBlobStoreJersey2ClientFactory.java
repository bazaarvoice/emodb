package com.bazaarvoice.emodb.blob.jersey2.client;

import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.client2.EmoClient;
import com.bazaarvoice.emodb.common.jersey2.RetryPolicy;

import java.io.Serializable;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract parent class for blob store clients.  Subclasses are expected to create and configure an
 * {@link EmoClient} and then supply it via the constructor.
 */
abstract public class AbstractBlobStoreJersey2ClientFactory implements Serializable {

    private final EmoClient _client;
    private URI _endPoint;
    private ScheduledExecutorService _connectionManagementService;

    protected AbstractBlobStoreJersey2ClientFactory(EmoClient client, URI endPoint) {
        _client = Objects.requireNonNull(client);
        _endPoint = Objects.requireNonNull(endPoint);
    }

    public BlobStore usingCredentials(final String apiKey) {
        AuthBlobStore authBlobStore = new BlobStoreJersey2Client(_endPoint, _client,
                _connectionManagementService, RetryPolicy.createDefault());
        return new BlobStoreJersey2AuthenticatorProxy(authBlobStore, apiKey);
    }

    protected void setConnectionManagementService(ScheduledExecutorService connectionManagementService) {
        _connectionManagementService = connectionManagementService;
    }
}
