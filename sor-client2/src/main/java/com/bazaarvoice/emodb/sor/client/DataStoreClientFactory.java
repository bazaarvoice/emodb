package com.bazaarvoice.emodb.sor.client;

import com.bazaarvoice.emodb.client2.EmoClient;
import com.bazaarvoice.emodb.client2.EmoClientException;
import com.bazaarvoice.emodb.client2.EmoResponse;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.jersey2.Jersey2EmoClient;
import com.bazaarvoice.emodb.common.json.JsonStreamProcessingException;
import com.bazaarvoice.emodb.common.json.JsonStreamingEOFException;
import com.bazaarvoice.emodb.sor.api.AuditSizeLimitException;
import com.bazaarvoice.emodb.sor.api.AuthDataStore;
import com.bazaarvoice.emodb.sor.api.DeltaSizeLimitException;
import com.bazaarvoice.emodb.sor.api.StashNotAvailableException;
import com.bazaarvoice.emodb.sor.api.TableExistsException;
import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.bazaarvoice.emodb.sor.api.UnknownTableException;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import dev.failsafe.RetryPolicy;
import org.glassfish.jersey.client.ClientProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.time.Duration;

/**
 * Connects to the System of Record using Jersey 2.x client.
 */
public class DataStoreClientFactory {
    private final EmoClient _client;
    private final URI _uri;
    private static final int MAX_RETRIES = 3;
    private static final long MIN_DELAY = 500;
    private static final long MAX_DELAY = 1000;
    private final Logger _log = LoggerFactory.getLogger(DataStoreClientFactory.class);


    public static DataStoreClientFactory forClusterAndHttpClient(URI endPoint, Client client) {
        client.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
        return new DataStoreClientFactory(endPoint, client);
    }

    private DataStoreClientFactory(URI endPoint, Client jerseyClient) {
        _uri = endPoint;
        _client = new Jersey2EmoClient(jerseyClient);
    }

    public AuthDataStore create() {
        return new DataStoreClient(_uri, _client, createRetryPolicy());
    }

    public RetryPolicy<Object> createRetryPolicy() {
        return RetryPolicy.builder()
                .handle(RuntimeException.class)
                .abortOn(exception -> !isRetriableException((Exception) exception))
                .withMaxRetries(MAX_RETRIES)
                .withBackoff(Duration.ofMillis(MIN_DELAY), Duration.ofMillis(MAX_DELAY))
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
        return ((e instanceof EmoClientException &&
                ((EmoClientException) e).getResponse().getStatus() >= 500) ||
                e instanceof JsonStreamingEOFException) ||
                (e instanceof WebApplicationException &&
                        ((WebApplicationException) e).getResponse().getStatus() >= 500) ||
                Throwables.getCausalChain(e).stream()
                        .anyMatch(Predicates.instanceOf(ProcessingException.class)::apply);
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private static RuntimeException convertException(EmoClientException e) {
        EmoResponse response = e.getResponse();
        String exceptionType = response.getFirstHeader("X-BV-Exception");

        if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()) {
            if (IllegalArgumentException.class.getName().equals(exceptionType)) {
                return new IllegalArgumentException(response.getEntity(String.class), e);
            } else if (JsonStreamProcessingException.class.getName().equals(exceptionType)) {
                return new JsonStreamProcessingException(response.getEntity(String.class));
            } else if (DeltaSizeLimitException.class.getName().equals(exceptionType)) {
                return response.getEntity(DeltaSizeLimitException.class);
            } else if (AuditSizeLimitException.class.getName().equals(exceptionType)) {
                return response.getEntity(AuditSizeLimitException.class);
            }

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
                UnknownPlacementException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(UnknownPlacementException.class).initCause(e);
            } else {
                return (RuntimeException) new UnknownPlacementException().initCause(e);
            }

        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                StashNotAvailableException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(StashNotAvailableException.class).initCause(e);
            } else {
                return (RuntimeException) new StashNotAvailableException().initCause(e);
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
