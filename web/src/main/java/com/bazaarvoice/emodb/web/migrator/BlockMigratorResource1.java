package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.web.migrator.migratorstatus.MigratorStatus;
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
    public MigratorStatus migrate(@PathParam("placement") String placement, @PathParam("id") String id) {

        if (_deltaMigrator.getStatus(id) != null) {
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
    public MigratorStatus getMigratorStatus(@PathParam ("id") String id) {
        MigratorStatus migratorStatus = _deltaMigrator.getStatus(id);

        if (migratorStatus == null) {
            throw new WebApplicationException(
                    Response.status(Response.Status.NOT_FOUND)
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .entity(ImmutableMap.of("not_found", id))
                            .build());
        }

        return migratorStatus;
    }

    @POST
    @Path ("migrate/{id}/cancel")
    public MigratorStatus cancelMigration(@PathParam ("id") String id) {
        MigratorStatus migratorStatus = _deltaMigrator.getStatus(id);
        if (migratorStatus == null) {
            throw new WebApplicationException(
                    Response.status(Response.Status.NOT_FOUND)
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .entity(ImmutableMap.of("not_found", id))
                            .build());
        }

        if (!migratorStatus.isCanceled()) {
            _deltaMigrator.cancel(id);
        }

        return _deltaMigrator.getStatus(id);
    }

    @POST
    @Path ("migrate/{id}/recover")
    public MigratorStatus recoverMigration(@PathParam ("id") String id) {
        MigratorStatus migratorStatus = _deltaMigrator.resubmitWorkflowTasks(id);
        if (migratorStatus == null) {
            throw new WebApplicationException(
                    Response.status(Response.Status.NOT_FOUND)
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .entity(ImmutableMap.of("not_found", id))
                            .build());
        }

        return migratorStatus;
    }
}
