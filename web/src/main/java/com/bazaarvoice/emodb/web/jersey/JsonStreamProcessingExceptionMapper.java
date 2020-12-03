package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.common.json.JsonStreamProcessingException;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class JsonStreamProcessingExceptionMapper implements ExceptionMapper<JsonStreamProcessingException> {
    @Override
    public Response toResponse(JsonStreamProcessingException e) {
        //noinspection ThrowableResultOfMethodCallIgnored
        Throwable rootCause = Throwables.getRootCause(e);

        return Response.status(Response.Status.BAD_REQUEST)
                .header("X-BV-Exception", JsonStreamProcessingException.class.getName())
                .entity(MoreObjects.firstNonNull(rootCause.getMessage(), "Invalid JSON request"))
                .type(MediaType.TEXT_PLAIN_TYPE)
                .build();
    }
}
