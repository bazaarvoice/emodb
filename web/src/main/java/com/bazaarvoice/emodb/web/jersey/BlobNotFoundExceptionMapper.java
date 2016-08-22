package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.blob.api.BlobNotFoundException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class BlobNotFoundExceptionMapper implements ExceptionMapper<BlobNotFoundException> {
    @Override
    public Response toResponse(BlobNotFoundException e) {
        return Response.status(Response.Status.NOT_FOUND)
                .header("X-BV-Exception", BlobNotFoundException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
