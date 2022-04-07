package com.bazaarvoice.emodb.blob.jersey2.client;

import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.client2.EmoClient;
import com.bazaarvoice.emodb.client2.EmoClientException;
import com.bazaarvoice.emodb.common.json.JsonStreamingEOFException;
import com.google.common.base.Preconditions;
import dev.failsafe.RetryPolicy;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract parent class for blob store clients.  Subclasses are expected to create and configure an
 * {@link EmoClient} and then supply it via the constructor.
 */
abstract public class AbstractBlobStoreJersey2ClientFactory implements Serializable {

    private final EmoClient _client;
    private URI _endPoint;
    private ScheduledExecutorService _connectionManagementService;
    private RetryPolicy<Object> _retryPolicy;

    protected AbstractBlobStoreJersey2ClientFactory(EmoClient client, URI endPoint) {
        _client = client;
        _endPoint = endPoint;
    }

    protected void setConnectionManagementService(ScheduledExecutorService connectionManagementService) {
        _connectionManagementService = connectionManagementService;
    }

    public boolean isRetriableException(Exception e) {
        return (e instanceof EmoClientException &&
                ((EmoClientException) e).getResponse().getStatus() >= 500) ||
                e instanceof JsonStreamingEOFException;
    }
    public BlobStore usingCredentials(final String apiKey) {
        if (_retryPolicy == null)
            createdDefaultRetryPolicy();
        AuthBlobStore authBlobStore = new BlobStoreJersey2Client(_endPoint, _client,
                _connectionManagementService, _retryPolicy);
        return new BlobStoreJersey2AuthenticatorProxy(authBlobStore, apiKey);
    }

    private void createdDefaultRetryPolicy() {
        _retryPolicy = RetryPolicy.builder()
                .withMaxRetries(3)
                .withBackoff( Duration.ofMillis(500), Duration.ofMillis(1000))
                .build();
    }

    protected <T extends AbstractBlobStoreJersey2ClientFactory> T withRetry(int maximumNumberOfRetry,
                                                                            long baseSleepTime, long maxSleepTime) {

        Preconditions.checkArgument(maximumNumberOfRetry <=5 && maximumNumberOfRetry > 0);
        Preconditions.checkArgument(baseSleepTime <= 1000L && baseSleepTime >= 100L);
        Preconditions.checkArgument(maxSleepTime <= 1000L && maxSleepTime >= 100L);

        _retryPolicy= RetryPolicy.builder()
                .withMaxRetries(maximumNumberOfRetry)
                .withBackoff((Duration.ofMillis(baseSleepTime)), (Duration.ofMillis(maxSleepTime)))
                .build();

        return (T) this;
    }
}
