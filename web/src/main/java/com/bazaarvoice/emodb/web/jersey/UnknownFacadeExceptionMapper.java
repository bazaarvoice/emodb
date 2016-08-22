package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.sor.api.UnknownFacadeException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class UnknownFacadeExceptionMapper implements ExceptionMapper<UnknownFacadeException> {
    @Override
    public Response toResponse(UnknownFacadeException e) {
        return Response.status(Response.Status.NOT_FOUND)
                .header("X-BV-Exception", UnknownFacadeException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
