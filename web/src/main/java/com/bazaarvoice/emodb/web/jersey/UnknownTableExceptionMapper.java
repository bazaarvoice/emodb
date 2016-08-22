package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.sor.api.UnknownTableException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class UnknownTableExceptionMapper implements ExceptionMapper<UnknownTableException> {
    @Override
    public Response toResponse(UnknownTableException e) {
        return Response.status(Response.Status.NOT_FOUND)
                .header("X-BV-Exception", UnknownTableException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
