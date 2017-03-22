package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.uac.api.EmoApiKeyNotFoundException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class EmoApiKeyNotFoundExceptionMapper implements ExceptionMapper<EmoApiKeyNotFoundException> {
    @Override
    public Response toResponse(EmoApiKeyNotFoundException e) {
        return Response.status(Response.Status.NOT_FOUND)
                .header("X-BV-Exception", EmoApiKeyNotFoundException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
