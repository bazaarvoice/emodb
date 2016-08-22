package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.databus.api.UnknownReplayException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class UnknownDatabusReplayExceptionMapper implements ExceptionMapper<UnknownReplayException> {
    @Override
    public Response toResponse(UnknownReplayException e) {
        return Response.status(Response.Status.NOT_FOUND)
                .header("X-BV-Exception", UnknownReplayException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}

