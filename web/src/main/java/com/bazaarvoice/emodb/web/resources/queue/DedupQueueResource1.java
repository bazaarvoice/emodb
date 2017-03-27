package com.bazaarvoice.emodb.web.resources.queue;

import com.bazaarvoice.emodb.auth.jersey.Authenticated;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.queue.api.DedupQueueService;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.MoveQueueStatus;
import com.bazaarvoice.emodb.queue.client.DedupQueueServiceAuthenticator;
import com.bazaarvoice.emodb.web.auth.Permissions;
import com.bazaarvoice.emodb.web.auth.resource.NamedResource;
import com.bazaarvoice.emodb.web.jersey.params.SecondsParam;
import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.dropwizard.jersey.params.LongParam;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.apache.shiro.authz.annotation.RequiresPermissions;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Path("/dedupq/1")
@Produces(MediaType.APPLICATION_JSON)
@RequiresAuthentication
@Api (value="DedupQueue: " , description = "All DedupQueue operations")
public class DedupQueueResource1 {

    private final DedupQueueService _queueService;
    private final DedupQueueServiceAuthenticator _queueClient;

    public DedupQueueResource1(DedupQueueService queueService, DedupQueueServiceAuthenticator queueClient) {
        _queueService = checkNotNull(queueService, "queueService");
        _queueClient = checkNotNull(queueClient, "queueClient");
    }

    @POST
    @Path("{queue}/send")
    @Consumes(MediaType.APPLICATION_JSON)
    @RequiresPermissions ("queue|post|{queue}")
    @Timed(name = "bv.emodb.dedupq.DedupQueueResource1.send", absolute = true)
    @ApiOperation (value = "Send Operation.",
            notes = "Returns a SuccessResponse.",
            response = SuccessResponse.class
    )
    public SuccessResponse send(@PathParam("queue") String queue, Object message) {
        // Not partitioned--any server can write messages to Cassandra.
        _queueService.send(queue, message);
        return SuccessResponse.instance();
    }

    @POST
    @Path("{queue}/sendbatch")
    @Consumes(MediaType.APPLICATION_JSON)
    @RequiresPermissions("queue|post|{queue}")
    @Timed(name = "bv.emodb.dedupq.DedupQueueResource1.sendBatch", absolute = true)
    @ApiOperation (value = "Send a Batch.",
            notes = "Returns a SuccessResponse",
            response = SuccessResponse.class
    )
    public SuccessResponse sendBatch(@PathParam("queue") String queue, Collection<Object> messages) {
        // Not partitioned--any server can write messages to Cassandra.
        _queueService.sendAll(queue, messages);
        return SuccessResponse.instance();
    }

    @POST
    @Path("_sendbatch")
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed(name = "bv.emodb.dedupq.DedupQueueResource1.sendBatches", absolute = true)
    @ApiOperation (value = "Send batches.",
            notes = "Returns a SuccessResponse.",
            response = SuccessResponse.class
    )
    public SuccessResponse sendBatches(Map<String, Collection<Object>> messagesByQueue,
                                       @Authenticated Subject subject) {
        for (String queue : messagesByQueue.keySet()) {
            if (!subject.hasPermission(Permissions.postQueue(new NamedResource(queue)))) {
                throw new WebApplicationException(
                        Response.status(Response.Status.FORBIDDEN)
                                .type(MediaType.APPLICATION_JSON_TYPE)
                                .entity(ImmutableMap.of("queue", queue))
                                .build());
            }
        }
        // Not partitioned--any server can write messages to Cassandra.
        _queueService.sendAll(messagesByQueue);
        return SuccessResponse.instance();
    }

    @GET
    @Path("{queue}/size")
    @RequiresPermissions("queue|get_status|{queue}")
    @Timed(name = "bv.emodb.dedupq.DedupQueueResource1.getMessageCount", absolute = true)
    @ApiOperation (value = "Gets the messsage count.",
            notes = "Returns a long.",
            response = long.class
    )
    public long getMessageCount(@QueryParam("partitioned") BooleanParam partitioned,
                                @PathParam("queue") String queue, @QueryParam("limit") LongParam limit,
                                @Authenticated Subject subject) {
        // Call different getMessageCount* methods to collect metrics data that distinguish limited vs. unlimited calls.
        if (limit == null || limit.get() == Long.MAX_VALUE) {
            return getService(partitioned, subject.getAuthenticationId()).getMessageCount(queue);
        } else {
            return getService(partitioned, subject.getAuthenticationId()).getMessageCountUpTo(queue, limit.get());
        }
    }


    @GET
    @Path("{queue}/claimcount")
    @RequiresPermissions("queue|get_status|{queue}")
    @Timed(name = "bv.emodb.dedupq.DedupQueueResource1.getClaimCount", absolute = true)
    @ApiOperation (value = "Gets the claim count.",
            notes = "Returns a long.",
            response = long.class
    )
    public long getClaimCount(@QueryParam("partitioned") BooleanParam partitioned,
                              @PathParam("queue") String queue,
                              @Authenticated Subject subject) {
        return getService(partitioned, subject.getAuthenticationId()).getClaimCount(queue);
    }

    @GET
    @Path("{queue}/peek")
    @RequiresPermissions("queue|poll|{queue}")
    @Timed(name = "bv.emodb.dedupq.DedupQueueResource1.peek", absolute = true)
    @ApiOperation (value = "Peek operation.",
            notes = "Returns a List of Messages.",
            response = Message.class
    )
    public List<Message> peek(@QueryParam("partitioned") BooleanParam partitioned,
                              @PathParam("queue") String queue,
                              @QueryParam("limit") @DefaultValue("10") IntParam limit,
                              @Authenticated Subject subject) {
        // Not partitioned.  Peeking ignores claims.
        return getService(partitioned, subject.getAuthenticationId()).peek(queue, limit.get());
    }

    @GET
    @Path("{queue}/poll")
    @RequiresPermissions("queue|poll|{queue}")
    @Timed(name = "bv.emodb.dedupq.DedupQueueResource1.poll", absolute = true)
    @ApiOperation (value = "Poll operation.",
            notes = "Returns a List of Messages",
            response = Message.class
    )
    public List<Message> poll(@QueryParam("partitioned") BooleanParam partitioned,
                              @PathParam("queue") String queue,
                              @QueryParam("ttl") @DefaultValue("30") SecondsParam claimTtl,
                              @QueryParam("limit") @DefaultValue("10") IntParam limit,
                              @Authenticated Subject subject) {
        return getService(partitioned, subject.getAuthenticationId()).poll(queue, claimTtl.get(), limit.get());
    }

    @POST
    @Path("{queue}/renew")
    @Consumes(MediaType.APPLICATION_JSON)
    @RequiresPermissions("queue|poll|{queue}")
    @Timed(name = "bv.emodb.dedupq.DedupQueueResource1.renew", absolute = true)
    @ApiOperation (value = "Renew operation.",
            notes = "Returns a SuccessResponse.",
            response = SuccessResponse.class
    )
    public SuccessResponse renew(@QueryParam("partitioned") BooleanParam partitioned,
                                 @PathParam("queue") String queue,
                                 @QueryParam("ttl") @DefaultValue("30") SecondsParam claimTtl,
                                 List<String> messageIds,
                                 @Authenticated Subject subject) {
        // Check for null parameters, which will throw a 400, otherwise it throws a 5xx error
        checkArgument(messageIds != null, "Missing message Ids");
        getService(partitioned, subject.getAuthenticationId()).renew(queue, messageIds, claimTtl.get());
        return SuccessResponse.instance();
    }

    @POST
    @Path("{queue}/ack")
    @Consumes(MediaType.APPLICATION_JSON)
    @RequiresPermissions("queue|poll|{queue}")
    @Timed(name = "bv.emodb.dedupq.DedupQueueResource1.acknowledge", absolute = true)
    @ApiOperation (value = "Acknowledge operation.",
            notes = "Returns a SuccessResponse",
            response = SuccessResponse.class
    )
    public SuccessResponse acknowledge(@QueryParam("partitioned") BooleanParam partitioned,
                                       @PathParam("queue") String queue,
                                       List<String> messageIds,
                                       @Authenticated Subject subject) {
        // Check for null parameters, which will throw a 400, otherwise it throws a 5xx error
        checkArgument(messageIds != null, "Missing message Ids");
        getService(partitioned, subject.getAuthenticationId()).acknowledge(queue, messageIds);
        return SuccessResponse.instance();
    }

    @POST
    @Path("_move")
    @RequiresPermissions({"queue|poll|{?from}", "queue|post|{?to}"})
    @Timed(name = "bv.emodb.dedupq.DedupQueueResource1.moveAsync", absolute = true)
    @ApiOperation (value = "Asynchronous Move operation.",
            notes = "Returns a Map.",
            response = Map.class
    )
    public Map<String, Object> moveAsync(@QueryParam("from") String from, @QueryParam("to") String to) {
        checkArgument(!Strings.isNullOrEmpty(from), "from is required");
        checkArgument(!Strings.isNullOrEmpty(to), "to is required");
        checkArgument(!from.equals(to), "cannot move queue to itself");

        String id = _queueService.moveAsync(from, to);
        return ImmutableMap.<String, Object>of("id", id);
    }

    @GET
    @Path("_move/{reference}")
    @Timed(name = "bv.emodb.dedupq.DedupQueueResource1.getMoveStatus", absolute = true)
    @ApiOperation (value = "gets the status of the Move operation.",
            notes = "Returns MoveQueueStatus object.",
            response = MoveQueueStatus.class
    )
    public MoveQueueStatus getMoveStatus(@PathParam("reference") String reference) {
        return _queueService.getMoveStatus(reference);
    }

    @POST
    @Path("{queue}/unclaimall")
    @RequiresPermissions("queue|poll|{queue}")
    @Timed(name = "bv.emodb.dedupq.DedupQueueResource1.unclaimAll", absolute = true)
    @ApiOperation (value = "Unclaim All operation.",
            notes = "Returns a SuccessResponse.",
            response = SuccessResponse.class
    )
    public SuccessResponse unclaimAll(@QueryParam("partitioned") BooleanParam partitioned,
                                      @PathParam("queue") String queue,
                                      @Authenticated Subject subject) {
        getService(partitioned, subject.getAuthenticationId()).unclaimAll(queue);
        return SuccessResponse.instance();
    }

    @DELETE
    @Path("{queue}")
    @RequiresPermissions("queue|poll|{queue}")
    @Timed(name = "bv.emodb.dedupq.DedupQueueResource1.purge", absolute = true)
    @ApiOperation (value = "Purge operation",
            notes = "Returns a SuccessResponse.",
            response = SuccessResponse.class
    )
    public SuccessResponse purge(@QueryParam("partitioned") BooleanParam partitioned,
                                 @PathParam("queue") String queue,
                                 @Authenticated Subject subject) {
        getService(partitioned, subject.getAuthenticationId()).purge(queue);
        return SuccessResponse.instance();
    }

    private DedupQueueService getService(BooleanParam partitioned, String apiKey) {
        return partitioned != null && partitioned.get() ? _queueService : _queueClient.usingCredentials(apiKey);
    }
}
