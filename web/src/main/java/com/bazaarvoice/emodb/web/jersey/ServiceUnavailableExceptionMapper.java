package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Exception mapper for when a resource explicitly returns that it is under heavy load or is otherwise unavailable
 * and the caller should try again later.
 */
@Provider
public class ServiceUnavailableExceptionMapper implements ExceptionMapper<ServiceUnavailableException> {

    @Override
    public Response toResponse(ServiceUnavailableException exception) {
        return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                .header("X-BV-Exception", ServiceUnavailableException.class.getName())
                .header("Retry-After", exception.getRetryAfterSeconds())
                .entity(exception)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
