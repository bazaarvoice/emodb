package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.auth.jersey.Authenticated;
import com.bazaarvoice.emodb.auth.jersey.Subject;
import com.bazaarvoice.emodb.common.json.LoggingIterator;
import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.EventViews;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.ReplaySubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.core.DatabusChannelConfiguration;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.client.DataStoreStreaming;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DatabusEventWriterRegistry;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.bazaarvoice.emodb.web.jersey.params.InstantParam;
import com.bazaarvoice.emodb.web.jersey.params.SecondsParam;
import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import com.bazaarvoice.emodb.web.resources.sor.ReadConsistencyParam;
import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import io.dropwizard.jersey.params.BooleanParam;
import io.dropwizard.jersey.params.IntParam;
import io.dropwizard.jersey.params.LongParam;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.shiro.authz.annotation.RequiresAuthentication;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Path ("/bus/1")
@Produces (MediaType.APPLICATION_JSON)
@RequiresAuthentication
@Api (value = "Databus: ", description = "All Databus operations")
public class DatabusResource1 {
    private static final Logger _log = LoggerFactory.getLogger(DatabusResource1.class);

    // Initialize helpers for returning client views of peek and poll response events
    private static final PeekOrPollResponseHelper _helperContentOnly = new PeekOrPollResponseHelper(EventViews.ContentOnly.class);
    private static final PeekOrPollResponseHelper _helperWithTags = new PeekOrPollResponseHelper(EventViews.WithTags.class);

    private final SubjectDatabus _databus;
    private final SubjectDatabus _databusClient;
    private final DatabusEventStore _eventStore;
    private final DatabusResourcePoller _poller;
    private final DataStore _dataStore;
    private final DatabusEventWriterRegistry _eventWriterRegistry;

    public DatabusResource1(SubjectDatabus databus,
                            SubjectDatabus databusClient,
                            DatabusEventStore eventStore,
                            DatabusEventWriterRegistry eventWriterRegistry,
                            DatabusResourcePoller databusResourcePoller,
                            DataStore dataStore) {
        _databus = checkNotNull(databus, "databus");
        _databusClient = checkNotNull(databusClient, "databusClient");
        _eventStore = checkNotNull(eventStore, "eventStore");
        _poller = checkNotNull(databusResourcePoller, "databusResourcePoller");
        _dataStore = checkNotNull(dataStore, "datastore");
        _eventWriterRegistry = checkNotNull(eventWriterRegistry, "eventWriterRegistry");
    }

    @Path ("_raw")
    public RawDatabusResource1 getRawResource() {
        return new RawDatabusResource1(_eventStore);
    }

    @GET
    @Timed (name = "bv.emodb.databus.DatabusResource1.listSubscription", absolute = true)
    @ApiOperation (value = "Lists Subscription.",
            notes = "Returns an Iterator of Subscription.",
            response = Subscription.class
    )
    public Iterator<Subscription> listSubscription(@QueryParam ("from") String fromKeyExclusive,
                                                   @QueryParam ("limit") @DefaultValue ("10") LongParam limit,
                                                   @Authenticated Subject subject) {
        return streamingIterator(_databus.listSubscriptions(subject, Strings.emptyToNull(fromKeyExclusive), limit.get()));
    }

    @PUT
    @Path ("{subscription}")
    @Consumes ("application/x.json-condition")
    @RequiresPermissions ("databus|subscribe|{subscription}")
    @Timed (name = "bv.emodb.databus.DatabusResource1.subscribe", absolute = true)
    @ApiOperation (value = "Subscribe operation.",
            notes = "Returns a SuccessResponse.",
            response = SuccessResponse.class
    )
    public SuccessResponse subscribe(@PathParam ("subscription") String subscription,
                                     String conditionString,
                                     @QueryParam ("ttl") @DefaultValue ("86400") SecondsParam subscriptionTtl,
                                     @QueryParam ("eventTtl") @DefaultValue ("86400") SecondsParam eventTtl,
                                     @QueryParam ("ignoreSuppressedEvents") BooleanParam ignoreSuppressedEventsParam,
                                     @QueryParam ("includeDefaultJoinFilter") BooleanParam includeDefaultJoinFilterParam,
                                     @Authenticated Subject subject) {

        // By default, include the default join filter condition
        // Note:  Historically this feature used to be called "ignoreSuppressedEvents".  To provide backwards
        //        compatibility both parameter names are accepted though precedence is given to the newer parameter.
        boolean includeDefaultJoinFilter =
                includeDefaultJoinFilterParam != null ? includeDefaultJoinFilterParam.get() :
                        (ignoreSuppressedEventsParam != null ? ignoreSuppressedEventsParam.get() : true);

        Condition tableFilter = Conditions.alwaysTrue();
        if (!conditionString.isEmpty()) {
            tableFilter = new ConditionParam(conditionString).get();
        }

        _databus.subscribe(subject, subscription, tableFilter, subscriptionTtl.get(), eventTtl.get(), includeDefaultJoinFilter);
        return SuccessResponse.instance();
    }

    @DELETE
    @Path ("{subscription}")
    @RequiresPermissions ("databus|unsubscribe|{subscription}")
    @Timed (name = "bv.emodb.databus.DatabusResource1.unsubscribe", absolute = true)
    @ApiOperation (value = "Unsubscribe operation.",
            notes = "Returns an Iterator of Subscription.",
            response = SuccessResponse.class
    )
    public SuccessResponse unsubscribe(@QueryParam ("partitioned") BooleanParam partitioned,
                                       @PathParam ("subscription") String subscription,
                                       @Authenticated Subject subject) {
        getClient(partitioned).unsubscribe(subject, subscription);
        return SuccessResponse.instance();
    }

    @GET
    @Path ("{subscription}")
    @Timed (name = "bv.emodb.databus.DatabusResource1.getSubscription", absolute = true)
    @ApiOperation (value = "Gets a Subscription.",
            notes = "Returns a Subscription.",
            response = Subscription.class
    )
    public Subscription getSubscription(@PathParam ("subscription") String subscription,
                                        @Authenticated Subject subject) {
        return _databus.getSubscription(subject, subscription);
    }

    @GET
    @Path ("{subscription}/size")
    @RequiresPermissions ("databus|get_status|{subscription}")
    @Timed (name = "bv.emodb.databus.DatabusResource1.getEventCount", absolute = true)
    @ApiOperation (value = "Gets the event count.",
            notes = "Returns a long.",
            response = long.class
    )
    public long getEventCount(@QueryParam ("partitioned") BooleanParam partitioned,
                              @PathParam ("subscription") String subscription, @QueryParam ("limit") LongParam limit,
                              @Authenticated Subject subject) {
        // Call different getEventCount* methods to collect metrics data that distinguish limited vs. unlimited calls.
        if (limit == null || limit.get() == Long.MAX_VALUE) {
            return getClient(partitioned).getEventCount(subject, subscription);
        } else {
            return getClient(partitioned).getEventCountUpTo(subject, subscription, limit.get());
        }
    }

    @GET
    @Path ("{subscription}/claimcount")
    @RequiresPermissions ("databus|get_status|{subscription}")
    @Timed (name = "bv.emodb.databus.DatabusResource1.getClaimCount", absolute = true)
    @ApiOperation (value = "Gets the claim count.",
            notes = "Returns a long.",
            response = long.class
    )
    public long getClaimCount(@QueryParam ("partitioned") BooleanParam partitioned,
                              @PathParam ("subscription") String subscription,
                              @Authenticated Subject subject) {
        return getClient(partitioned).getClaimCount(subject, subscription);
    }

    @GET
    @Path ("{subscription}/peek")
    @RequiresPermissions ("databus|poll|{subscription}")
    @Timed (name = "bv.emodb.databus.DatabusResource1.peek", absolute = true)
    @ApiOperation (value = "Peek operation.",
            notes = "Returns an List of Events.",
            response = Event.class
    )
    public Response peek(@QueryParam ("partitioned") BooleanParam partitioned,
                         @PathParam ("subscription") String subscription,
                         @QueryParam ("limit") @DefaultValue ("10") IntParam limit,
                         @QueryParam ("includeTags") @DefaultValue ("false") BooleanParam includeTags,
                         @Authenticated Subject subject) {
        // For backwards compatibility with older clients only include tags if explicitly requested
        // (default is false).
        PeekOrPollResponseHelper helper = getPeekOrPollResponseHelper(includeTags.get());
        Iterator<Event> events = getClient(partitioned).peek(subject, subscription, limit.get());
        return Response.ok().entity(helper.asEntity(events)).build();
    }

    @GET
    @Path ("{subscription}/poll")
    @RequiresPermissions ("databus|poll|{subscription}")
    @ApiOperation (value = "poll operation.",
            notes = "Returns a Response.",
            response = Response.class
    )
    public Response poll(@QueryParam ("partitioned") BooleanParam partitioned,
                         @PathParam ("subscription") String subscription,
                         @QueryParam ("ttl") @DefaultValue ("30") SecondsParam claimTtl,
                         @QueryParam ("limit") @DefaultValue ("10") IntParam limit,
                         @QueryParam ("ignoreLongPoll") @DefaultValue ("false") BooleanParam ignoreLongPoll,
                         @QueryParam ("includeTags") @DefaultValue ("false") BooleanParam includeTags,
                         @Context HttpServletRequest request,
                         @Authenticated Subject subject) {
        // For backwards compatibility with older clients only include tags if explicitly requested
        // (default is false).
        PeekOrPollResponseHelper helper = getPeekOrPollResponseHelper(includeTags.get());
        return _poller.poll(subject, getClient(partitioned), subscription, claimTtl.get(), limit.get(), request,
                ignoreLongPoll.get(), helper);
    }

    private PeekOrPollResponseHelper getPeekOrPollResponseHelper(boolean includeTags) {
        return includeTags ? _helperWithTags : _helperContentOnly;
    }

    @POST
    @Path ("{subscription}/renew")
    @Consumes (MediaType.APPLICATION_JSON)
    @RequiresPermissions ("databus|poll|{subscription}")
    @Timed (name = "bv.emodb.databus.DatabusResource1.renew", absolute = true)
    @ApiOperation (value = "Renew operation.",
            notes = "Returns a SucessResponse.",
            response = SuccessResponse.class
    )
    public SuccessResponse renew(@QueryParam ("partitioned") BooleanParam partitioned,
                                 @PathParam ("subscription") String subscription,
                                 @QueryParam ("ttl") @DefaultValue ("30") SecondsParam claimTtl,
                                 List<String> eventKeys,
                                 @Authenticated Subject subject) {
        getClient(partitioned).renew(subject, subscription, eventKeys, claimTtl.get());
        return SuccessResponse.instance();
    }

    @POST
    @Path ("{subscription}/ack")
    @Consumes (MediaType.APPLICATION_JSON)
    @RequiresPermissions ("databus|poll|{subscription}")
    @Timed (name = "bv.emodb.databus.DatabusResource1.acknowledge", absolute = true)
    @ApiOperation (value = "Acknowledge operation.",
            notes = "Returns a SucessResponse.",
            response = SuccessResponse.class
    )
    public SuccessResponse acknowledge(@QueryParam ("partitioned") BooleanParam partitioned,
                                       @PathParam ("subscription") String subscription,
                                       List<String> eventKeys,
                                       @Authenticated Subject subject) {
        // Check for null parameters, which will throw a 400, otherwise it throws a 5xx error
        checkArgument(eventKeys != null, "Missing event keys");
        getClient(partitioned).acknowledge(subject, subscription, eventKeys);
        return SuccessResponse.instance();
    }

    @POST
    @Path ("{subscription}/replay")
    @RequiresPermissions ("databus|poll|{subscription}")
    @Timed (name = "bv.emodb.databus.DatabusResource1.replay", absolute = true)
    @ApiOperation (value = "Replay operation.",
            notes = "Returns a Map.",
            response = Map.class
    )
    public Map<String, Object> replay(@PathParam ("subscription") String subscription,
                                      @QueryParam ("since") InstantParam sinceParam,
                                      @Authenticated Subject subject) {
        checkArgument(!Strings.isNullOrEmpty(subscription), "subscription is required");
        Instant since = (sinceParam == null) ? null : sinceParam.get();
        // Make sure since is within Replay TTL
        checkArgument(since == null || since.plus(DatabusChannelConfiguration.REPLAY_TTL).isAfter(Instant.now()),
                "Since timestamp is outside the replay TTL. Use null 'since' if you want to replay all events.");
        String id = _databus.replayAsyncSince(subject, subscription, since != null ? Date.from(since) : null);
        return ImmutableMap.<String, Object>of("id", id);
    }

    @GET
    @Path ("_replay/{replayId}")
    @Timed (name = "bv.emodb.databus.DatabusResource1.getReplayStatus", absolute = true)
    @ApiOperation (value = "gets the status of the Replay operation.",
            notes = "Returns a ReplaySubsciptionStatus.",
            response = ReplaySubscriptionStatus.class
    )
    public ReplaySubscriptionStatus getReplayStatus(@PathParam ("replayId") String replayId,
                                                    @Authenticated Subject subject) {
        return _databus.getReplayStatus(subject, replayId);
    }

    @POST
    @Path ("_move")
    @RequiresPermissions ({"databus|poll|{?from}", "databus|subscribe|{?to}"})
    @Timed (name = "bv.emodb.databus.DatabusResource1.move", absolute = true)
    @ApiOperation (value = "Move operation.",
            notes = "Returns a Map.",
            response = Map.class
    )
    public Map<String, Object> move(@QueryParam ("from") String from, @QueryParam ("to") String to,
                                    @Authenticated Subject subject) {
        checkArgument(!Strings.isNullOrEmpty(from), "from is required");
        checkArgument(!Strings.isNullOrEmpty(to), "to is required");
        checkArgument(!from.equals(to), "cannot move subscription to itself");

        String id = _databus.moveAsync(subject, from, to);
        return ImmutableMap.<String, Object>of("id", id);
    }

    @GET
    @Path ("_move/{reference}")
    @Timed (name = "bv.emodb.databus.DatabusResource1.getMoveStatus", absolute = true)
    @ApiOperation (value = "gets the status of the Move operation.",
            notes = "Returns a MoveSubscriptionStatus.",
            response = MoveSubscriptionStatus.class
    )
    public MoveSubscriptionStatus getMoveStatus(@PathParam ("reference") String reference,
                                                @Authenticated Subject subject) {
        return _databus.getMoveStatus(subject, reference);
    }

    @POST
    @Path ("{subscription}/inject")
    @RequiresPermissions ("databus|inject|{subscription}")
    @Timed (name = "bv.emodb.databus.DatabusResource1.injectEvent", absolute = true)
    @ApiOperation (value = "Injects an event.",
            notes = "Returns a SuccessResponse.",
            response = SuccessResponse.class
    )
    public SuccessResponse injectEvent(@PathParam ("subscription") String subscription,
                                       @QueryParam ("table") String table,
                                       @QueryParam ("key") String key,
                                       @Authenticated Subject subject) {
        // Not partitioned--any server can write events to Cassandra.
        _databus.injectEvent(subject, subscription, table, key);
        return SuccessResponse.instance();
    }

    @POST
    @Path ("{subscription}/unclaimall")
    @RequiresPermissions ("databus|poll|{subscription}")
    @Timed (name = "bv.emodb.databus.DatabusResource1.unclaimAll", absolute = true)
    @ApiOperation (value = "Unclaims All.",
            notes = "Returns a SuccessResponse.",
            response = SuccessResponse.class
    )
    public SuccessResponse unclaimAll(@QueryParam ("partitioned") BooleanParam partitioned,
                                      @PathParam ("subscription") String subscription,
                                      @Authenticated Subject subject) {
        getClient(partitioned).unclaimAll(subject, subscription);
        return SuccessResponse.instance();
    }

    @POST
    @Path ("{subscription}/purge")
    @RequiresPermissions ("databus|poll|{subscription}")
    @Timed (name = "bv.emodb.databus.DatabusResource1.purge", absolute = true)
    @ApiOperation (value = "Purge operation.",
            notes = "Returns a SuccessResponse.",
            response = SuccessResponse.class
    )
    public SuccessResponse purge(@QueryParam ("partitioned") BooleanParam partitioned,
                                 @PathParam ("subscription") String subscription,
                                 @Authenticated Subject subject) {
        getClient(partitioned).purge(subject, subscription);
        return SuccessResponse.instance();
    }

    private SubjectDatabus getClient(BooleanParam partitioned) {
        return partitioned != null && partitioned.get() ? _databus : _databusClient;
    }

    private static <T> Iterator<T> streamingIterator(Iterator<T> iterator) {
        // Force the calculation of at least the first item in the iterator so that, if an exception occurs, we find
        // out before writing the HTTP response code & headers.  Otherwise we will at best report a 500 error instead
        // of applying Jersey exception mappings and maybe returning a 400 error etc.
        PeekingIterator<T> peekingIterator = Iterators.peekingIterator(iterator);
        if (peekingIterator.hasNext()) {
            peekingIterator.peek();
        }

        return new LoggingIterator<>(peekingIterator, _log);
    }

    @POST
    @Path ("_batch-write-events")
    @RequiresPermissions ("system|raw_databus")
    @Timed (name = "bv.emodb.sor.DatabusResource1.writeEvents", absolute = true)
    @ApiOperation (value = "Batch replay operation.",
            notes = "Batch replay operation.",
            response = SuccessResponse.class
    )
    public SuccessResponse writeEvents(Collection<UpdateRef> refs,
                                       @Authenticated Subject subject) {
        _eventWriterRegistry.getDatabusWriter().writeEvents(refs);
        return SuccessResponse.instance();
    }

    @POST
    @Path ("_write-table-events/{table}")
    @RequiresPermissions ("sor|read|{table}, system|raw_databus")
    @Timed (name = "bv.emodb.sor.DatabusResource1.writeTableEvent", absolute = true)
    @ApiOperation (value = "Replay operation.",
            notes = "Replay operation.",
            response = Iterator.class
    )
    public Iterator<UpdateRef> writeEvents(@PathParam ("table") String table,
                                        @QueryParam ("consistency") @DefaultValue ("STRONG") ReadConsistencyParam consistency,
                                        @QueryParam ("batchsize") @DefaultValue ("10000") IntParam batchSize,
                                        @QueryParam ("since") InstantParam sinceParam,
                                        @Authenticated Subject subject) {
        checkArgument(!Strings.isNullOrEmpty(table), "table is required");
        Instant since = (sinceParam == null) ? null : sinceParam.get();
        Date date = since != null ? Date.from(since) : null;
        Iterator<UpdateRef> result = Iterators.emptyIterator();
        Collection<String> splits = _dataStore.getSplits(table, batchSize.get());
        Predicate<Map<String, Object>> predicate = getDatePredicate(date);
        splits.parallelStream().forEach(split -> {
            Iterable<Map<String, Object>> documents = DataStoreStreaming.getSplit(_dataStore, table, split, true, consistency.get());
            List<UpdateRef> updateRefs = StreamSupport.stream(documents.spliterator(), true)
                    .filter(predicate)
                    .map(document -> getUpdateRef(document))
                    .collect(Collectors.toList());

            _eventWriterRegistry.getDatabusWriter().writeEvents(updateRefs);
            Iterators.concat(result, updateRefs.iterator());
        });
        return streamingIterator(result);
    }

    private static Predicate<Map<String, Object>> getDatePredicate(Date date) {
        Predicate<Map<String, Object>> datePredicate = document -> Intrinsic.getLastUpdateAt(document).compareTo(date) >= 0;
        Predicate<Map<String, Object>> alwaysTruePredicate = document -> true;
        return date == null ? alwaysTruePredicate : datePredicate;
    }

    @POST
    @Path ("_write-key-event/{table}/{key}")
//    @RequiresPermissions ("sor|read|{table}, system|raw_databus")
    @Timed (name = "bv.emodb.sor.DatabusResource1.writeEvent", absolute = true)
    @ApiOperation (value = "Replay operation.",
            notes = "Replay operation.",
            response = UpdateRef.class
    )
    public UpdateRef sendEvent(@PathParam ("table") String table,
                               @PathParam ("key") String key,
                               @QueryParam ("consistency") @DefaultValue ("STRONG") ReadConsistencyParam consistency,
                               @QueryParam ("since") InstantParam sinceParam,
                               @Authenticated Subject subject) {
        checkArgument(!Strings.isNullOrEmpty(table), "table is required");
        checkArgument(!Strings.isNullOrEmpty(key), "key is required");

        Instant since = (sinceParam == null) ? null : sinceParam.get();
        Date date = since != null ? Date.from(since) : null;
        Map<String, Object> document = _dataStore.get(table, key, consistency.get());
        UpdateRef updateRef = null;
        // send events only if sinceParam isn't specified, or
        // document had been created before sinceParam
        if (getDatePredicate(date).test(document)) {
            updateRef = getUpdateRef(document);
            _eventWriterRegistry.getDatabusWriter().writeEvents(Collections.singleton(updateRef));
        }
        return updateRef;
    }

    private static UpdateRef getUpdateRef(final Map<String, Object> document) {
        Set<String> tags = (Set<String>) document.get(UpdateRef.TAGS_NAME);
        //determine changedId base on lastUpdateAt timestamp
        UUID changeId = TimeUUIDs.uuidForTimestamp(Intrinsic.getLastUpdateAt(document));

        return new UpdateRef(Intrinsic.getTable(document), Intrinsic.getId(document), changeId, tags == null ? new HashSet<>() : tags);
    }
}
