package com.bazaarvoice.emodb.auth.jersey;

import com.bazaarvoice.emodb.common.api.UnauthorizedException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

/**
 * Exception mapper for UnauthorizedException when thrown directly by a resource.
 */
@Provider
public class UnauthorizedExceptionMapper implements ExceptionMapper<UnauthorizedException> {

    @Override
    public Response toResponse(UnauthorizedException exception) {
        return Response.status(Response.Status.FORBIDDEN)
                .header("X-BV-Exception", UnauthorizedException.class.getName())
                .type(MediaType.APPLICATION_JSON_TYPE)
                .entity(exception)
                .build();
    }
}