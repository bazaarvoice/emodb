package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.databus.api.UnknownMoveException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class UnknownDatabusMoveExceptionMapper implements ExceptionMapper<UnknownMoveException> {
    @Override
    public Response toResponse(UnknownMoveException e) {
        return Response.status(Response.Status.NOT_FOUND)
                .header("X-BV-Exception", UnknownMoveException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
