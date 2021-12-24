package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.sor.api.AuditsUnavailableException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class AuditsUnavailableExceptionMapper implements ExceptionMapper<AuditsUnavailableException> {
    @Override
    public Response toResponse(AuditsUnavailableException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                .header("X-BV-Exception", AuditsUnavailableException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}