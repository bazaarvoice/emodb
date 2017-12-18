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

import static com.google.common.base.Preconditions.checkArgument;

@Path("/migrator/1")
@Produces(MediaType.APPLICATION_JSON)
public class BlockMigratorResource1 {

    private final DeltaMigrator _deltaMigrator;

    public BlockMigratorResource1(DeltaMigrator deltaMigrator) {
        _deltaMigrator = deltaMigrator;
    }


    @POST
    @Path ("migrate/{placement}")
    @Timed(name = "bv.emodb.sor.DataStoreResource1.migrate", absolute = true)
    @ApiOperation(value = "Migrates deltas to new block tables.",
            notes = "Migrates deltas to new block tables.",
            response = SuccessResponse.class
    )
    public MigratorStatus migrate(@PathParam("placement") String placement,
                                  @QueryParam("maxWritesPerSecond") int maxWritesPerSecond) {

        if (_deltaMigrator.getStatus(placement) != null) {
            throw new WebApplicationException(
                    Response.status(Response.Status.CONFLICT)
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .entity(ImmutableMap.of("Migration Exists", placement))
                            .build());
        }

        return _deltaMigrator.migratePlacement(placement, maxWritesPerSecond);

    }

    @GET
    @Path ("migrate/{placement}")
    public MigratorStatus getMigratorStatus(@PathParam ("placement") String placement) {
        MigratorStatus migratorStatus = _deltaMigrator.getStatus(placement);

        if (migratorStatus == null) {
            throw new WebApplicationException(
                    Response.status(Response.Status.NOT_FOUND)
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .entity(ImmutableMap.of("not_found", placement))
                            .build());
        }

        return migratorStatus;
    }

    @POST
    @Path ("migrate/{placement}/cancel")
    public MigratorStatus cancelMigration(@PathParam ("placement") String placement) {
        MigratorStatus migratorStatus = _deltaMigrator.getStatus(placement);
        if (migratorStatus == null) {
            throw new WebApplicationException(
                    Response.status(Response.Status.NOT_FOUND)
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .entity(ImmutableMap.of("not_found", placement))
                            .build());
        }

        if (!migratorStatus.isCanceled()) {
            _deltaMigrator.cancel(placement);
        }

        return _deltaMigrator.getStatus(placement);
    }

    @POST
    @Path ("migrate/{placement}/recover")
    public MigratorStatus recoverMigration(@PathParam ("placement") String placement) {
        MigratorStatus migratorStatus = _deltaMigrator.resubmitWorkflowTasks(placement);
        if (migratorStatus == null) {
            throw new WebApplicationException(
                    Response.status(Response.Status.NOT_FOUND)
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .entity(ImmutableMap.of("not_found", placement))
                            .build());
        }

        return migratorStatus;
    }

    @POST
    @Path ("migrate/{placement}/throttle")
    public MigratorStatus throttleMigration(@PathParam ("placement") String placement,
                                            @QueryParam ("maxWritesPerSecond") int maxWritesPerSecond) {
        checkArgument(maxWritesPerSecond > 0, "maxWritesPerSecond is required");

        MigratorStatus migratorStatus = _deltaMigrator.getStatus(placement);
        if (migratorStatus == null) {
            throw new WebApplicationException(
                    Response.status(Response.Status.NOT_FOUND)
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .entity(ImmutableMap.of("not_found", placement))
                            .build());
        }

        _deltaMigrator.throttle(placement, maxWritesPerSecond);

        return _deltaMigrator.getStatus(placement);

    }
}
