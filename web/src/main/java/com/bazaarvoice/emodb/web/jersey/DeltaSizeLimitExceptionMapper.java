package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.sor.api.DeltaSizeLimitException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class DeltaSizeLimitExceptionMapper implements ExceptionMapper<DeltaSizeLimitException> {
    @Override
    public Response toResponse(DeltaSizeLimitException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                .header("X-BV-Exception", DeltaSizeLimitException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
