package com.bazaarvoice.emodb.web.jersey;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class UnsupportedOperationExceptionMapper implements ExceptionMapper<UnsupportedOperationException> {

    @Override
    public Response toResponse(UnsupportedOperationException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                .header("X-BV-Exception", UnsupportedOperationException.class.getName())
                .entity(new UnsupportedOperationException(e.getMessage()))
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}