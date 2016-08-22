package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.sortedq.core.ReadOnlyQueueException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class ReadOnlyQueueExceptionMapper implements ExceptionMapper<ReadOnlyQueueException> {
    @Override
    public Response toResponse(ReadOnlyQueueException e) {
        // Don't re-throw this exception on the client side.  It's internal.
        return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                .header("X-BV-Exception", ReadOnlyQueueException.class.getName())
                .entity("Server does not manage the specified resource at this time.")
                .type(MediaType.TEXT_PLAIN_TYPE)
                .build();
    }
}
