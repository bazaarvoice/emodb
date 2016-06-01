package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.databus.api.UnauthorizedSubscriptionException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class UnauthorizedSubscriptionExceptionMapper implements ExceptionMapper<UnauthorizedSubscriptionException> {
    @Override
    public Response toResponse(UnauthorizedSubscriptionException e) {
        return Response.status(Response.Status.FORBIDDEN)
                .header("X-BV-Exception", UnauthorizedSubscriptionException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
