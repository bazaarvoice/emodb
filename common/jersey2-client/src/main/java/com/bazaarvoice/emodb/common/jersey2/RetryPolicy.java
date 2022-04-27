package com.bazaarvoice.emodb.common.jersey2;

import com.bazaarvoice.emodb.client2.EmoClientException;
import com.bazaarvoice.emodb.client2.EmoResponse;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.json.JsonStreamProcessingException;
import com.bazaarvoice.emodb.common.json.JsonStreamingEOFException;
import com.bazaarvoice.emodb.uac.api.EmoApiKeyExistsException;
import com.bazaarvoice.emodb.uac.api.EmoApiKeyNotFoundException;
import com.bazaarvoice.emodb.uac.api.EmoRoleExistsException;
import com.bazaarvoice.emodb.uac.api.EmoRoleNotFoundException;
import com.bazaarvoice.emodb.uac.api.InsufficientRolePermissionException;
import com.bazaarvoice.emodb.uac.api.InvalidEmoPermissionException;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.time.Duration;

public final class RetryPolicy {

    private static final int MAX_RETRIES = 3;
    private static final long MIN_DELAY = 500;
    private static final long MAX_DELAY = 1000;

    private static final Logger _log = LoggerFactory.getLogger(RetryPolicy.class);

    private RetryPolicy() {}

    public static dev.failsafe.RetryPolicy createDefault() {
        return dev.failsafe.RetryPolicy.builder()
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

    private static boolean isRetriableException(Exception e) {
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

