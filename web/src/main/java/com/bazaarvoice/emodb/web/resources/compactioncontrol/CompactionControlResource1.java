package com.bazaarvoice.emodb.web.resources.compactioncontrol;

import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import com.google.common.base.Strings;
import io.dropwizard.jersey.params.LongParam;
import org.apache.shiro.authz.annotation.RequiresPermissions;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Produces (MediaType.APPLICATION_JSON)
public class CompactionControlResource1 {

    private final CompactionControlSource _compactionControlSource;

    public CompactionControlResource1(CompactionControlSource compactionControlSource) {
        _compactionControlSource = checkNotNull(compactionControlSource, "compactionControlSource");
    }

    @POST
    @Path ("/stash-time/{id}")
    @RequiresPermissions ("system|comp_control")
    public SuccessResponse updateStashTime(@PathParam ("id") String id,
                                           @QueryParam ("timestamp") LongParam timestampInMillisParam,
                                           @QueryParam ("placement") List<String> placements,
                                           @QueryParam ("expiredTimestamp") LongParam expiredTimestampInMillisParam,
                                           @QueryParam ("dataCenter") String dataCenter) {
        checkArgument(timestampInMillisParam != null, "timestamp is required");
        checkArgument(!placements.isEmpty(), "Placement is required");
        checkArgument(expiredTimestampInMillisParam != null, "expired timestamp is required.");

        _compactionControlSource.updateStashTime(id, timestampInMillisParam.get(), placements, expiredTimestampInMillisParam.get(), dataCenter);
        return SuccessResponse.instance();
    }

    @DELETE
    @Path ("/stash-time/{id}")
    @RequiresPermissions ("system|comp_control")
    public SuccessResponse deleteStashTime(@PathParam ("id") String id, @QueryParam ("dataCenter") String dataCenter) {
        checkArgument(!Strings.isNullOrEmpty(id), "id is required");
        checkArgument(!Strings.isNullOrEmpty(dataCenter), "datacenter is required");

        _compactionControlSource.deleteStashTime(id, dataCenter);
        return SuccessResponse.instance();
    }

    @GET
    @Path ("/stash-time/{id}")
    @RequiresPermissions ("system|comp_control")
    public StashRunTimeInfo getStashTime(@PathParam ("id") String id, @QueryParam ("dataCenter") String dataCenter) {
        checkArgument(!Strings.isNullOrEmpty(id), "id is required");
        checkArgument(!Strings.isNullOrEmpty(dataCenter), "datacenter is required");

        StashRunTimeInfo stashTimeInfo = _compactionControlSource.getStashTime(id, dataCenter);
        return (stashTimeInfo != null) ? stashTimeInfo : new StashRunTimeInfo();
    }

    @GET
    @Path ("/stash-time")
    @RequiresPermissions ("system|comp_control")
    public Map<String, StashRunTimeInfo> getStashTimesForPlacement(@QueryParam ("placement") String placement) {
        return Strings.isNullOrEmpty(placement) ? _compactionControlSource.getAllStashTimes() : _compactionControlSource.getStashTimesForPlacement(placement);
    }
}