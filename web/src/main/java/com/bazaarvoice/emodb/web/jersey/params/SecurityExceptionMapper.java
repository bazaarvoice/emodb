package com.bazaarvoice.emodb.web.jersey.params;

import com.google.common.base.Objects;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class SecurityExceptionMapper implements ExceptionMapper<SecurityException> {
    @Override
    public Response toResponse(SecurityException e) {
        return Response.status(Response.Status.FORBIDDEN)
                .header("X-BV-Exception", SecurityException.class.getName())
                .entity(Objects.firstNonNull(e.getMessage(), "Access Denied."))
                .type(MediaType.TEXT_PLAIN_TYPE)
                .build();
    }
}
