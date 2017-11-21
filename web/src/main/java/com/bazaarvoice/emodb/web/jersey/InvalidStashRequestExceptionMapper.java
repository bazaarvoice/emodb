package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.sor.api.InvalidStashRequestException;
import com.bazaarvoice.emodb.uac.api.EmoRoleNotFoundException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class InvalidStashRequestExceptionMapper implements ExceptionMapper<InvalidStashRequestException> {
    @Override
    public Response toResponse(InvalidStashRequestException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                .header("X-BV-Exception", InvalidStashRequestException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}