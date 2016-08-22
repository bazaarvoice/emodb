package com.bazaarvoice.emodb.web.scanner.resource;

import com.bazaarvoice.emodb.web.scanner.ScanDestination;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.ScanUploader;
import com.bazaarvoice.emodb.web.scanner.scanstatus.ScanStatus;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

@Path ("/scanner/1")
@Produces (MediaType.APPLICATION_JSON)
@Consumes (MediaType.WILDCARD)
public class ScanUploadResource1 {

    private final ScanUploader _scanUploader;

    public ScanUploadResource1(ScanUploader scanUploader) {
        _scanUploader = scanUploader;
    }

    @POST
    @Path ("upload/{id}")
    public ScanStatus startScan(@PathParam ("id") String id,
                                @QueryParam ("placement") List<String> placements,
                                @QueryParam ("dest") List<String> destinationParams,
                                @QueryParam ("byAZ") @DefaultValue ("true") Boolean byAZ,
                                @QueryParam ("maxConcurrency") @DefaultValue ("4") Integer maxConcurrency,
                                @QueryParam ("compactionEnabled") @DefaultValue ("false") Boolean compactionEnabled,
                                @QueryParam ("dryRun") @DefaultValue ("false") Boolean dryRun) {

        checkArgument(!placements.isEmpty(), "Placement is required");
        checkArgument(!destinationParams.isEmpty(), "One or more destinations is required");

        if (_scanUploader.getStatus(id) != null) {
            throw new WebApplicationException(
                    Response.status(Response.Status.CONFLICT)
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .entity(ImmutableMap.of("scan_exists", id))
                            .build());
        }

        List<ScanDestination> destinations = Lists.transform(destinationParams, new Function<String, ScanDestination>() {
            @Override
            public ScanDestination apply(String destination) {
                if ("null".equals(destination)) {
                    return ScanDestination.discard();
                }
                return ScanDestination.to(URI.create(destination));
            }
        });

        ScanOptions options = new ScanOptions(placements)
                .addDestinations(destinations)
                .setScanByAZ(byAZ)
                .setMaxConcurrentSubRangeScans(maxConcurrency)
                .setCompactionEnabled(compactionEnabled);

        return _scanUploader.scanAndUpload(id, options, dryRun);
    }

    @GET
    @Path ("upload/{id}")
    public ScanStatus getScanStatus(@PathParam ("id") String id) {
        ScanStatus scanStatus = _scanUploader.getStatus(id);

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
        ScanStatus scanStatus = _scanUploader.getStatus(id);
        if (scanStatus == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        if (!scanStatus.isCanceled()) {
            _scanUploader.cancel(id);
        }

        return _scanUploader.getStatus(id);
    }

    @POST
    @Path ("upload/{id}/recover")
    public ScanStatus recoverScan(@PathParam ("id") String id) {
        ScanStatus scanStatus = _scanUploader.resubmitWorkflowTasks(id);
        if (scanStatus == null) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }

        return scanStatus;
    }
}
