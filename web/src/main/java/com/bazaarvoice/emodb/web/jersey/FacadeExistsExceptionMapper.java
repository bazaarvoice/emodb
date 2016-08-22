package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.sor.api.FacadeExistsException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class FacadeExistsExceptionMapper implements ExceptionMapper<FacadeExistsException> {
    @Override
    public Response toResponse(FacadeExistsException e) {
        return Response.status(Response.Status.CONFLICT)
                .header("X-BV-Exception", FacadeExistsException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
