package com.bazaarvoice.emodb.uac.client2;

import com.bazaarvoice.emodb.client2.EmoClient;
import com.bazaarvoice.emodb.client2.EmoClientException;
import com.bazaarvoice.emodb.client2.EmoResponse;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.jersey2.Jersey2EmoClient;
import com.bazaarvoice.emodb.common.json.JsonStreamProcessingException;
import com.bazaarvoice.emodb.common.json.JsonStreamingEOFException;
import com.bazaarvoice.emodb.uac.api.AuthUserAccessControl;
import com.bazaarvoice.emodb.uac.api.EmoApiKeyExistsException;
import com.bazaarvoice.emodb.uac.api.EmoApiKeyNotFoundException;
import com.bazaarvoice.emodb.uac.api.EmoRoleExistsException;
import com.bazaarvoice.emodb.uac.api.EmoRoleNotFoundException;
import com.bazaarvoice.emodb.uac.api.InsufficientRolePermissionException;
import com.bazaarvoice.emodb.uac.api.InvalidEmoPermissionException;
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
 * Simple Factory for creating {@link AuthUserAccessControl} REST clients independent of Ostrich library.
 */
public class UserAccessControlClientFactory  {
    private final EmoClient _client;
    private final URI _uri;
    private static final int MAX_RETRIES = 3;
    private static final long MIN_DELAY = 500;
    private static final long MAX_DELAY = 1000;
    private final Logger _log = LoggerFactory.getLogger(UserAccessControlClientFactory.class);


    public static UserAccessControlClientFactory forClusterAndHttpClient(URI endPoint, Client jerseyClient) {
        jerseyClient.property(ClientProperties.SUPPRESS_HTTP_COMPLIANCE_VALIDATION, true);
        return new UserAccessControlClientFactory(endPoint, jerseyClient);
    }


    private UserAccessControlClientFactory(URI endPoint, Client jerseyClient) {
        _uri = endPoint;
        _client = new Jersey2EmoClient(jerseyClient);
    }

    public AuthUserAccessControl create() {
        return new UserAccessControlClient(_uri, _client, createRetryPolicy());
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
    private RuntimeException convertException(EmoClientException e) {
        EmoResponse response = e.getResponse();
        String exceptionType = response.getFirstHeader("X-BV-Exception");

        if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode()) {
            if (InvalidEmoPermissionException.class.getName().equals(exceptionType)) {
                if (response.hasEntity()) {
                    return (RuntimeException) response.getEntity(InvalidEmoPermissionException.class).initCause(e);
                } else {
                    return (RuntimeException) new InvalidEmoPermissionException().initCause(e);
                }
            } else if (IllegalArgumentException.class.getName().equals(exceptionType)) {
                return new IllegalArgumentException(response.getEntity(String.class), e);
            } else if (JsonStreamProcessingException.class.getName().equals(exceptionType)) {
                return new JsonStreamProcessingException(response.getEntity(String.class));
            }

        } else if (response.getStatus() == Response.Status.CONFLICT.getStatusCode()) {
            if (EmoApiKeyExistsException.class.getName().equals(exceptionType)) {
                if (response.hasEntity()) {
                    return (RuntimeException) response.getEntity(EmoApiKeyExistsException.class).initCause(e);
                } else {
                    return (RuntimeException) new EmoApiKeyExistsException().initCause(e);
                }
            } else if (EmoRoleExistsException.class.getName().equals(exceptionType)) {
                if (response.hasEntity()) {
                    return (RuntimeException) response.getEntity(EmoRoleExistsException.class).initCause(e);
                } else {
                    return (RuntimeException) new EmoRoleExistsException().initCause(e);
                }
            }

        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode()) {
            if (EmoApiKeyNotFoundException.class.getName().equals(exceptionType)) {
                if (response.hasEntity()) {
                    return (RuntimeException) response.getEntity(EmoApiKeyNotFoundException.class).initCause(e);
                } else {
                    return (RuntimeException) new EmoApiKeyNotFoundException().initCause(e);
                }
            } else if (EmoRoleNotFoundException.class.getName().equals(exceptionType)) {
                if (response.hasEntity()) {
                    return (RuntimeException) response.getEntity(EmoRoleNotFoundException.class).initCause(e);
                } else {
                    return (RuntimeException) new EmoRoleNotFoundException().initCause(e);
                }
            }

        } else if (response.getStatus() == Response.Status.FORBIDDEN.getStatusCode()) {
            if (UnauthorizedException.class.getName().equals(exceptionType)) {
                if (response.hasEntity()) {
                    return (RuntimeException) response.getEntity(UnauthorizedException.class).initCause(e);
                } else {
                    return (RuntimeException) new UnauthorizedException().initCause(e);
                }
            } else if (InsufficientRolePermissionException.class.getName().equals(exceptionType)) {
                if (response.hasEntity()) {
                    return (RuntimeException) response.getEntity(InsufficientRolePermissionException.class).initCause(e);
                } else {
                    return (RuntimeException) new InsufficientRolePermissionException().initCause(e);
                }
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
