package com.bazaarvoice.emodb.web.resources.queue;

import com.bazaarvoice.emodb.auth.jersey.Authenticated;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.MoveQueueStatus;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.emodb.queue.client.QueueServiceAuthenticator;
import com.bazaarvoice.emodb.web.auth.Permissions;
import com.bazaarvoice.emodb.web.auth.resource.NamedResource;
import com.bazaarvoice.emodb.web.jersey.params.SecondsParam;
import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Path("/queue/1")
@Produces(MediaType.APPLICATION_JSON)
@RequiresAuthentication
@Api (value="Queue: " , description = "All Queue operations")
public class QueueResource1 {

    //private final MetricRegistry _metricRegistry;
    private final QueueService _queueService;
    private final QueueServiceAuthenticator _queueClient;
    private final Meter _messageCount_qr1;
    private final Meter _nullPollsCount_qr1;

    private final Meter _sendCount_qr1;
    private final Meter _sendNullCount_qr1;

    private final Meter _sendBatch_qr1;

    private final Meter _sendBatchNull_qr1;

    public QueueResource1(QueueService queueService, QueueServiceAuthenticator queueClient, MetricRegistry metricRegistry) {
        //this._metricRegistry = metricRegistry;

        _queueService = requireNonNull(queueService, "queueService");
        _queueClient = requireNonNull(queueClient, "queueClient");
        _messageCount_qr1 = metricRegistry.meter(MetricRegistry.name(QueueResource1.class, "polledMessageCount_qr1"));
        _nullPollsCount_qr1 = metricRegistry.meter(MetricRegistry.name(QueueResource1.class, "nullPollsCount_qr1"));
        _sendCount_qr1= metricRegistry.meter(MetricRegistry.name(QueueResource1.class,"sendCount_qr1"));
        _sendNullCount_qr1= metricRegistry.meter(MetricRegistry.name(QueueResource1.class,"sendNullCount_qr1"));
        _sendBatch_qr1= metricRegistry.meter(MetricRegistry.name(QueueResource1.class,"sendBatch_qr1"));
        _sendBatchNull_qr1= metricRegistry.meter(MetricRegistry.name(QueueResource1.class,"sendBatchNull_qr1"));

    }

    @POST
    @Path("{queue}/send")
    @Consumes(MediaType.APPLICATION_JSON)
    @RequiresPermissions("queue|post|{queue}")
    @Timed(name = "bv.emodb.queue.QueueResource1.send", absolute = true)
    @ApiOperation (value = "Send operation.",
            notes = "Returns a SuccessResponse.",
            response = SuccessResponse.class
    )
    public SuccessResponse send(@PathParam("queue") String queue, Object message) {
        // Not partitioned--any server can write messages to Cassandra.

        if (message == null) {
            _sendNullCount_qr1.mark();
        }
        else{
            _sendCount_qr1.mark();
        }
        _queueService.send(queue, message);
        return SuccessResponse.instance();
    }

    @POST
    @Path("{queue}/sendbatch")
    @Consumes(MediaType.APPLICATION_JSON)
    @RequiresPermissions("queue|post|{queue}")
    @Timed(name = "bv.emodb.queue.QueueResource1.sendBatch", absolute = true)
    @ApiOperation (value = "Send a Batch.",
            notes = "Returns a SuccessResponse..",
            response = SuccessResponse.class
    )
    public SuccessResponse sendBatch(@PathParam("queue") String queue, Collection<Object> messages) {

        if (messages == null || messages.isEmpty()) {
            _sendBatchNull_qr1.mark(); // Increment the sendnull meter
        }
        else {
            _sendBatch_qr1.mark(messages.size());
        }
        // Not partitioned--any server can write messages to Cassandra.
        _queueService.sendAll(queue, messages);
        return SuccessResponse.instance();
    }


// endpoint to write to cassandra after throttled messages come from kafka
    @POST
    @Path("{queue}/sendbatch1")
    @Consumes(MediaType.APPLICATION_JSON)
    @RequiresPermissions("queue|post|{queue}")
    @Timed(name = "bv.emodb.queue.QueueResource1.sendBatch", absolute = true)
    @ApiOperation (value = "Send a Batch.",
            notes = "Returns a SuccessResponse..",
            response = SuccessResponse.class
    )
    public SuccessResponse sendBatch1(@PathParam("queue") String queue, Collection<Object> events) {
        //TODO change query param name / type
        // Not partitioned--any server can write messages to Cassandra.
        _queueService.sendAll(queue, events, true);
        return SuccessResponse.instance();
    }

    @POST
    @Path("_sendbatch")
    @Consumes(MediaType.APPLICATION_JSON)
    @Timed(name = "bv.emodb.queue.QueueResource1.sendBatches", absolute = true)
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
    @Timed(name = "bv.emodb.queue.QueueResource1.getMessageCount", absolute = true)
    @ApiOperation (value = "gets the Message count.",
            notes = "Returns a long.",
            response = long.class
    )
    public long getMessageCount(@PathParam("queue") String queue, @QueryParam("limit") LongParam limit) {
        // Not partitioned--any server can count messages in Cassandra.  Claims are ignored.
        // Call different getMessageCount* methods to collect metrics data that distinguish limited vs. unlimited calls.
        if (limit == null || limit.get() == Long.MAX_VALUE) {
            return _queueService.getMessageCount(queue);
        } else {
            return _queueService.getMessageCountUpTo(queue, limit.get());
        }
    }


    @GET
    @Path("{queue}/claimcount")
    @RequiresPermissions("queue|get_status|{queue}")
    @Timed(name = "bv.emodb.queue.QueueResource1.getClaimCount", absolute = true)
    @ApiOperation (value = "Gets the claim count",
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
    @Timed(name = "bv.emodb.queue.QueueResource1.peek", absolute = true)
    @ApiOperation (value = "Peek operation.",
            notes = "Returns a List of Messages",
            response = Message.class
    )
    public List<Message> peek(@PathParam("queue") String queue,
                              @QueryParam("limit") @DefaultValue("10") IntParam limit) {
        // Not partitioned.  Peeking ignores claims.
        return _queueService.peek(queue, limit.get());
    }

    @GET
    @Path("{queue}/poll")
    @RequiresPermissions("queue|poll|{queue}")
    @Timed(name = "bv.emodb.queue.QueueResource1.poll", absolute = true)
    @ApiOperation (value = "Poll operation",
            notes = "Returns a List of Messages.",
            response = Message.class
    )
    public List<Message> poll(@QueryParam("partitioned") BooleanParam partitioned,
                              @PathParam("queue") String queue,
                              @QueryParam("ttl") @DefaultValue("30") SecondsParam claimTtl,
                              @QueryParam("limit") @DefaultValue("10") IntParam limit,
                              @Authenticated Subject subject) {
        List<Message> polledMessages = getService(partitioned, subject.getAuthenticationId()).poll(queue, claimTtl.get(), limit.get());
        if(polledMessages.isEmpty()){
            _nullPollsCount_qr1.mark();
        }
        else{
            _messageCount_qr1.mark(polledMessages.size());
        }
        return polledMessages;
    }

    @POST
    @Path("{queue}/renew")
    @Consumes(MediaType.APPLICATION_JSON)
    @RequiresPermissions("queue|poll|{queue}")
    @Timed(name = "bv.emodb.queue.QueueResource1.renew", absolute = true)
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
    @Timed(name = "bv.emodb.queue.QueueResource1.acknowledge", absolute = true)
    @ApiOperation (value = "Acknowledge operation.",
            notes = "Returns a SuccessResponse..",
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
    @Timed(name = "bv.emodb.queue.QueueResource1.moveAsync", absolute = true)
    @ApiOperation (value = "Asynchronous move operation.",
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
    @Timed(name = "bv.emodb.queue.QueueResource1.getMoveStatus", absolute = true)
    @ApiOperation (value = "Gets the Move operation status",
            notes = "Returns a MoveQueueStatus.",
            response = MoveQueueStatus.class
    )
    public MoveQueueStatus getMoveStatus(@PathParam("reference") String reference) {
        return _queueService.getMoveStatus(reference);
    }

    @POST
    @Path("{queue}/unclaimall")
    @RequiresPermissions("queue|poll|{queue}")
    @Timed(name = "bv.emodb.queue.QueueResource1.unclaimAll", absolute = true)
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
    @Timed(name = "bv.emodb.queue.QueueResource1.purge", absolute = true)
    @ApiOperation (value = "Purge operation.",
            notes = "Returns a SuccessResponse.",
            response = SuccessResponse.class
    )
    public SuccessResponse purge(@QueryParam("partitioned") BooleanParam partitioned,
                                 @PathParam("queue") String queue,
                                 @Authenticated Subject subject) {
        getService(partitioned, subject.getAuthenticationId()).purge(queue);
        return SuccessResponse.instance();
    }

    private QueueService getService(BooleanParam partitioned, String apiKey) {
        return partitioned != null && partitioned.get() ? _queueService : _queueClient.usingCredentials(apiKey);
    }
}
