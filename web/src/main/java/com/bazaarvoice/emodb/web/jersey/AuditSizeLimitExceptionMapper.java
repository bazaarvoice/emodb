package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.sor.api.AuditSizeLimitException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class AuditSizeLimitExceptionMapper implements ExceptionMapper<AuditSizeLimitException> {
    @Override
    public Response toResponse(AuditSizeLimitException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                .header("X-BV-Exception", AuditSizeLimitException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}