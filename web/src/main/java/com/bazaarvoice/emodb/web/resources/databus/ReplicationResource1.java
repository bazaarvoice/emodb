package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.databus.repl.ReplicationEvent;
import com.bazaarvoice.emodb.databus.repl.ReplicationSource;
import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import com.codahale.metrics.annotation.Timed;
import io.dropwizard.jersey.params.IntParam;
import org.apache.shiro.authz.annotation.RequiresPermissions;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Path("/busrepl/1")
@Produces(MediaType.APPLICATION_JSON)
public class ReplicationResource1 {
    private final ReplicationSource _replicationSource;

    public ReplicationResource1(ReplicationSource replicationSource) {
        _replicationSource = checkNotNull(replicationSource, "replicationSource");
    }

    @GET
    @Path("{channel}")
    @RequiresPermissions("system|replicate_databus")
    @Timed(name = "bv.emodb.databus.ReplicationResource1.get", absolute = true)
    public List<ReplicationEvent> get(@PathParam("channel") String channel,
                                      @QueryParam("limit") @DefaultValue("100") IntParam limit) {
        return _replicationSource.get(channel, limit.get());
    }

    @POST
    @Path("{channel}/ack")
    @Consumes(MediaType.APPLICATION_JSON)
    @RequiresPermissions("system|replicate_databus")
    @Timed(name = "bv.emodb.databus.ReplicationResource1.delete", absolute = true)
    public SuccessResponse delete(@PathParam("channel") String channel,
                                  List<String> messageIds) {
        _replicationSource.delete(channel, messageIds);
        return SuccessResponse.instance();
    }
}
