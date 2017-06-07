package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.uac.api.EmoRoleExistsException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class EmoRoleExistsExceptionMapper implements ExceptionMapper<EmoRoleExistsException> {
    @Override
    public Response toResponse(EmoRoleExistsException e) {
        return Response.status(Response.Status.CONFLICT)
                .header("X-BV-Exception", EmoRoleExistsException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}