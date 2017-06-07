package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.uac.api.EmoApiKeyExistsException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class EmoApiKeyExistsExceptionMapper implements ExceptionMapper<EmoApiKeyExistsException> {
    @Override
    public Response toResponse(EmoApiKeyExistsException e) {
        return Response.status(Response.Status.CONFLICT)
                .header("X-BV-Exception", EmoApiKeyExistsException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
