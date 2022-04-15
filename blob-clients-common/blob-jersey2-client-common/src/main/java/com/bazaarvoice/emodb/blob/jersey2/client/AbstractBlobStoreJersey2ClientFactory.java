package com.bazaarvoice.emodb.blob.jersey2.client;

import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.client2.EmoClient;
import com.bazaarvoice.emodb.client2.EmoClientException;
import com.bazaarvoice.emodb.common.json.JsonStreamingEOFException;
import dev.failsafe.RetryPolicy;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
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
                _connectionManagementService, createRetryPolicy());
        return new BlobStoreJersey2AuthenticatorProxy(authBlobStore, apiKey);
    }

    public RetryPolicy<Object> createRetryPolicy() {
//  FIXME      isRetriableException(Exception e) or it's variation should be used in handleIf or another method
//        RetryPolicy.builder()
//                .handle(RuntimeException.class)
//                .withMaxRetries(retryPolicy.getConfig().getMaxRetries())
//                .withBackoff(retryPolicy.getConfig().getDelay(), retryPolicy.getConfig().getMaxDelay())
//                .onRetry(e -> {
//                    Throwable ex = e.getLastException();
//                    _log.warn("Exception occurred: "+ex.getMessage()+ " Applying retry policy");
//                })
//                .onFailure(e -> {
//                    Throwable ex = e.getException();
//                    _log.error("Failed to execute the request due to the exception: " +ex);
//                    convertException((EmoClientException) e.getException());
//                })
//                .build();
        return RetryPolicy.builder()
                .withMaxRetries(3)
                .withBackoff(Duration.ofMillis(500), Duration.ofMillis(1000))
                .build();
    }

    public boolean isRetriableException(Exception e) {
        return (e instanceof EmoClientException &&
                ((EmoClientException) e).getResponse().getStatus() >= 500) ||
                e instanceof JsonStreamingEOFException;
    }

    protected void setConnectionManagementService(ScheduledExecutorService connectionManagementService) {
        _connectionManagementService = connectionManagementService;
    }
}
