package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class UnknownSubscriptionExceptionMapper implements ExceptionMapper<UnknownSubscriptionException> {
    @Override
    public Response toResponse(UnknownSubscriptionException e) {
        return Response.status(Response.Status.NOT_FOUND)
                .header("X-BV-Exception", UnknownSubscriptionException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
