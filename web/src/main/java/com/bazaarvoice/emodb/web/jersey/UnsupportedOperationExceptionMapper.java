package com.bazaarvoice.emodb.web.jersey;

import com.google.common.base.Objects;

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
                .type(MediaType.TEXT_PLAIN_TYPE)
                .entity(Objects.firstNonNull(e.getMessage(), "Unsupported Operation."))
                .type(MediaType.TEXT_PLAIN_TYPE)
                .build();
    }
}