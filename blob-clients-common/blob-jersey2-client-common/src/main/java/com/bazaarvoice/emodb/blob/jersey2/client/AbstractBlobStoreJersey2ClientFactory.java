package com.bazaarvoice.emodb.blob.jersey2.client;

import com.bazaarvoice.emodb.blob.api.AuthBlobStore;
import com.bazaarvoice.emodb.blob.api.BlobNotFoundException;
import com.bazaarvoice.emodb.blob.api.BlobStore;
import com.bazaarvoice.emodb.blob.api.RangeNotSatisfiableException;
import com.bazaarvoice.emodb.client2.EmoClient;
import com.bazaarvoice.emodb.client2.EmoClientException;
import com.bazaarvoice.emodb.client2.EmoResponse;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.json.JsonStreamingEOFException;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import dev.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
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
    private static final int maxRetries = 3;
    private static final long minDelay = 500;
    private static final long maxDelay = 1000;
    private final Logger _log = LoggerFactory.getLogger(getClass());

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
        return RetryPolicy.builder()
                .handle(RuntimeException.class)
                .handleIf(exception -> isRetriableException((Exception) exception))
                .withMaxRetries(maxRetries)
                .withBackoff(Duration.ofMillis(minDelay), Duration.ofMillis(maxDelay))
                .onRetry(e -> {
                    Throwable ex = e.getLastException();
                    _log.warn("Exception occurred: " + ex.getMessage() + " Applying retry policy");
                })
                .onFailure(e -> {
                    Throwable ex = e.getException();
                    _log.error("Failed to execute the request due to the exception: " + ex);
                    convertException((EmoClientException) e.getException());
                })
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

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private RuntimeException convertException(EmoClientException e) {
        EmoResponse response = e.getResponse();
        String exceptionType = response.getFirstHeader("X-BV-Exception");

        if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode() &&
                IllegalArgumentException.class.getName().equals(exceptionType)) {
            return new IllegalArgumentException(response.getEntity(String.class), e);

        } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode() &&
                TableExistsException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(TableExistsException.class).initCause(e);
            } else {
                return (RuntimeException) new TableExistsException().initCause(e);
            }

        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                UnknownTableException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(UnknownTableException.class).initCause(e);
            } else {
                return (RuntimeException) new UnknownTableException().initCause(e);
            }

        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                BlobNotFoundException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(BlobNotFoundException.class).initCause(e);
            } else {
                return (RuntimeException) new BlobNotFoundException().initCause(e);
            }
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                UnknownPlacementException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(UnknownPlacementException.class).initCause(e);
            } else {
                return (RuntimeException) new UnknownPlacementException().initCause(e);
            }

        } else if (response.getStatus() == 416 /* REQUESTED_RANGE_NOT_SATIFIABLE */ &&
                RangeNotSatisfiableException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(RangeNotSatisfiableException.class).initCause(e);
            } else {
                return (RuntimeException) new RangeNotSatisfiableException(null, -1, -1).initCause(e);
            }

        } else if (response.getStatus() == Response.Status.MOVED_PERMANENTLY.getStatusCode() &&
                UnsupportedOperationException.class.getName().equals(exceptionType)) {
            return new UnsupportedOperationException("Permanent redirect: " + response.getLocation(), e);

        } else if (response.getStatus() == Response.Status.FORBIDDEN.getStatusCode() &&
                UnauthorizedException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(UnauthorizedException.class).initCause(e);
            } else {
                return (RuntimeException) new UnauthorizedException().initCause(e);
            }
        } else if (response.getStatus() == Response.Status.SERVICE_UNAVAILABLE.getStatusCode() &&
                ServiceUnavailableException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(ServiceUnavailableException.class).initCause(e);
            } else {
                return (RuntimeException) new ServiceUnavailableException().initCause(e);
            }
        }

        return e;
    }
}
