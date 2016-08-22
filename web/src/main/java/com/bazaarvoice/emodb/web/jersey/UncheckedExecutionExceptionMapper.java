package com.bazaarvoice.emodb.web.jersey;

import com.google.common.util.concurrent.UncheckedExecutionException;
import io.dropwizard.jersey.errors.LoggingExceptionMapper;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.Providers;

/**
 * Guava wraps exceptions thrown by{@link com.google.common.util.concurrent.Futures#getUnchecked(java.util.concurrent.Future)}
 * and {@link com.google.common.cache.LoadingCache#getUnchecked(Object)} in an {@link UncheckedExecutionException}.
 * If the underlying cause of the exception has a custom exception mapper, unwrap the {@code UncheckedExecutionException}
 * and delegate to the custom exception mapper.
 */
@Provider
public class UncheckedExecutionExceptionMapper implements ExceptionMapper<UncheckedExecutionException> {
    private final Providers _providers;

    public UncheckedExecutionExceptionMapper(@Context Providers providers) {
        _providers = providers;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Response toResponse(UncheckedExecutionException e) {
        ExceptionMapper mapper = _providers.getExceptionMapper(e.getCause().getClass());
        if (mapper == null) {
            return null;
        } else if (mapper instanceof LoggingExceptionMapper) {
            return mapper.toResponse(e);
        } else {
            return mapper.toResponse(e.getCause());
        }
    }
}
