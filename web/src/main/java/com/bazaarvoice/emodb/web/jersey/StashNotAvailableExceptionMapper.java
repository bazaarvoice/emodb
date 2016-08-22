package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.sor.api.StashNotAvailableException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class StashNotAvailableExceptionMapper implements ExceptionMapper<StashNotAvailableException> {
    @Override
    public Response toResponse(StashNotAvailableException e) {
        return Response.status(Response.Status.NOT_FOUND)
                .header("X-BV-Exception", StashNotAvailableException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
