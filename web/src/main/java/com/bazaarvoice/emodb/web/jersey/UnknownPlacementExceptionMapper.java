package com.bazaarvoice.emodb.web.jersey;

import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import static java.lang.String.format;

public class UnknownPlacementExceptionMapper implements ExceptionMapper<UnknownPlacementException> {
    @Override
    public Response toResponse(UnknownPlacementException e) {
        // This exception commonly has a message which is not intended for external users.  Change the exception
        // message to suit our audience.

        String message = e.getTable() != null ?
            format("Table %s is stored in a locally inaccessible placement: %s", e.getTable(), e.getPlacement()) :
            format("Placement is locally inaccessible: %s", e.getPlacement());

        return Response.status(Response.Status.NOT_FOUND)
                .header("X-BV-Exception", UnknownPlacementException.class.getName())
                .entity(new UnknownPlacementException(message, e.getPlacement(), e.getTable()))
                .type(MediaType.APPLICATION_JSON_TYPE)
                .build();
    }
}
