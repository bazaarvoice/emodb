package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.uac.api.InvalidEmoPermissionException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class InvalidEmoPermissionExceptionMapper implements ExceptionMapper<InvalidEmoPermissionException> {
    @Override
    public Response toResponse(InvalidEmoPermissionException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                .header("X-BV-Exception", InvalidEmoPermissionException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
