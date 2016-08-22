package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.blob.api.RangeNotSatisfiableException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class RangeNotSatisfiableExceptionMapper implements ExceptionMapper<RangeNotSatisfiableException> {
    @Override
    public Response toResponse(RangeNotSatisfiableException e) {
        return Response.status(416)
                .header("X-BV-Exception", RangeNotSatisfiableException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
