package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.codahale.metrics.annotation.Timed;
import com.google.common.collect.ImmutableMap;
import io.swagger.annotations.ApiOperation;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/migrator/1")
@Produces(MediaType.APPLICATION_JSON)
public class BlockMigratorResource1 {

    private final DeltaMigrator _deltaMigrator;

    public BlockMigratorResource1(DeltaMigrator deltaMigrator) {
        _deltaMigrator = deltaMigrator;
    }


    @POST
    @Path ("migrate/{placement}/{id}")
    @Timed(name = "bv.emodb.sor.DataStoreResource1.migrate", absolute = true)
    @ApiOperation(value = "Migrates deltas to new block tables.",
            notes = "Migrates deltas to new block tables.",
            response = SuccessResponse.class
    )
    public ScanStatus migrate(@PathParam("placement") String placement, @PathParam("id") String id) {

        if (_deltaMigrator.getStatus("id") != null) {
            throw new WebApplicationException(
                    Response.status(Response.Status.CONFLICT)
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .entity(ImmutableMap.of("Migration Exists", placement))
                            .build());
        }

        return _deltaMigrator.migratePlacement(placement, id);

    }

    @GET
    @Path ("migrate/{id}")
    public ScanStatus getScanStatus(@PathParam ("id") String id) {
        ScanStatus scanStatus = _deltaMigrator.getStatus(id);

        if (scanStatus == null) {
            throw new WebApplicationException(
                    Response.status(Response.Status.NOT_FOUND)
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .entity(ImmutableMap.of("not_found", id))
                            .build());
        }

        return scanStatus;
    }

    @POST
    @Path ("upload/{id}/cancel")
    public ScanStatus cancelScan(@PathParam ("id") String id) {
        ScanStatus scanStatus = _deltaMigrator.getStatus(id);
        if (scanStatus == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        if (!scanStatus.isCanceled()) {
            _deltaMigrator.cancel(id);
        }

        return _deltaMigrator.getStatus(id);
    }

    @POST
    @Path ("upload/{id}/recover")
    public ScanStatus recoverScan(@PathParam ("id") String id) {
        ScanStatus scanStatus = _deltaMigrator.resubmitWorkflowTasks(id);
        if (scanStatus == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        return scanStatus;
    }
}
