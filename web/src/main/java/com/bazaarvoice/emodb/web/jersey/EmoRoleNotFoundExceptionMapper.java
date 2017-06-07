package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.uac.api.EmoRoleNotFoundException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class EmoRoleNotFoundExceptionMapper implements ExceptionMapper<EmoRoleNotFoundException> {
    @Override
    public Response toResponse(EmoRoleNotFoundException e) {
        return Response.status(Response.Status.NOT_FOUND)
                .header("X-BV-Exception", EmoRoleNotFoundException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
