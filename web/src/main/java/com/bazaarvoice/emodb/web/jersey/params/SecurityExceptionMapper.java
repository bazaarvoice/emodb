package com.bazaarvoice.emodb.web.jersey.params;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import java.util.Optional;

@Provider
public class SecurityExceptionMapper implements ExceptionMapper<SecurityException> {
    @Override
    public Response toResponse(SecurityException e) {
        return Response.status(Response.Status.FORBIDDEN)
                .header("X-BV-Exception", SecurityException.class.getName())
                .entity(Optional.ofNullable(e.getMessage()).orElse("Access Denied."))
                .type(MediaType.TEXT_PLAIN_TYPE)
                .build();
    }
}
