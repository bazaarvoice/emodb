package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.uac.api.InsufficientRolePermissionException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

public class InsufficientRolePermissionExceptionMapper implements ExceptionMapper<InsufficientRolePermissionException> {
    @Override
    public Response toResponse(InsufficientRolePermissionException e) {
        return Response.status(Response.Status.FORBIDDEN)
                .header("X-BV-Exception", InsufficientRolePermissionException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
