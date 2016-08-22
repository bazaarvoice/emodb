package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.sor.api.TableExistsException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class TableExistsExceptionMapper implements ExceptionMapper<TableExistsException> {
    @Override
    public Response toResponse(TableExistsException e) {
        return Response.status(Response.Status.CONFLICT)
                .header("X-BV-Exception", TableExistsException.class.getName())
                .entity(e)
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
