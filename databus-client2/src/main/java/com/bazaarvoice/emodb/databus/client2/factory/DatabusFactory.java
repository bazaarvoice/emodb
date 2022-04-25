package com.bazaarvoice.emodb.databus.client2.factory;

import com.bazaarvoice.emodb.client2.EmoClient;
import com.bazaarvoice.emodb.client2.EmoClientException;
import com.bazaarvoice.emodb.client2.EmoResponse;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.jersey2.Jersey2EmoClient;
import com.bazaarvoice.emodb.common.json.JsonStreamingEOFException;
import com.bazaarvoice.emodb.databus.api.UnauthorizedSubscriptionException;
import com.bazaarvoice.emodb.databus.api.UnknownMoveException;
import com.bazaarvoice.emodb.databus.api.UnknownReplayException;
import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;
import com.bazaarvoice.emodb.databus.client2.client.DatabusClient;
import com.bazaarvoice.emodb.databus.client2.discovery.EmoServiceDiscovery;
import com.bazaarvoice.emodb.databus.client2.discovery.ZKEmoServiceDiscovery;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Service;
import dev.failsafe.RetryPolicy;
import org.glassfish.jersey.client.JerseyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.Serializable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * Factory for creating {@link DatabusClient} clients.
 */
public class DatabusFactory implements Serializable {

    private final Logger _log = LoggerFactory.getLogger(DatabusFactory.class);

    private final EmoServiceDiscovery _emoServiceDiscovery;
    private final String _apiKey;
    private final EmoClient _emoClient;
    private static final int MAX_RETRIES = 3;
    private static final long MIN_DELAY = 500;
    private static final long MAX_DELAY = 1000;

    public DatabusFactory(EmoServiceDiscovery emoServiceDiscovery, String apiKey, JerseyClient client) {
        _emoServiceDiscovery = requireNonNull(emoServiceDiscovery, "Service discovery is required");
        _apiKey = requireNonNull(apiKey, "API key is required");
        _emoClient = new Jersey2EmoClient(requireNonNull(client, "Client is required"));
    }

    public DatabusClient create() {
        if (_emoServiceDiscovery instanceof ZKEmoServiceDiscovery) {
            Service service = ((ZKEmoServiceDiscovery) _emoServiceDiscovery).startAsync();

            try {
                service.awaitRunning(30, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                _log.error("Databus discovery did not start in a reasonable time");
                throw Throwables.propagate(e);
            } catch (Exception e) {
                _log.error("Databus discovery startup failed", e);
            }
        }
        return new DatabusClient(_emoServiceDiscovery, _emoClient, _apiKey, createRetryPolicy());
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

    private RuntimeException convertException(EmoClientException e) {
        EmoResponse response = e.getResponse();
        String exceptionType = response.getFirstHeader("X-BV-Exception");

        if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode() &&
                IllegalArgumentException.class.getName().equals(exceptionType)) {
            return new IllegalArgumentException(response.getEntity(String.class), e);

        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                UnknownSubscriptionException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(UnknownSubscriptionException.class).initCause(e);
            } else {
                return (RuntimeException) new UnknownSubscriptionException().initCause(e);
            }

        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                UnknownMoveException.class.getName().equals(exceptionType)) {
            return response.getEntity(UnknownMoveException.class);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                UnknownReplayException.class.getName().equals(exceptionType)) {
            return response.getEntity(UnknownReplayException.class);
        } else if (response.getStatus() == Response.Status.FORBIDDEN.getStatusCode() &&
                UnauthorizedSubscriptionException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(UnauthorizedSubscriptionException.class).initCause(e);
            } else {
                return (RuntimeException) new UnauthorizedSubscriptionException().initCause(e);
            }
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
