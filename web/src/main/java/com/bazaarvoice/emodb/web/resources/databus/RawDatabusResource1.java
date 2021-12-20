package com.bazaarvoice.emodb.web.resources.databus;

import com.bazaarvoice.emodb.common.uuid.TimeUUIDs;
import com.bazaarvoice.emodb.databus.api.Names;
import com.bazaarvoice.emodb.databus.core.DatabusEventStore;
import com.bazaarvoice.emodb.databus.core.UpdateRefSerializer;
import com.bazaarvoice.emodb.event.api.EventData;
import com.bazaarvoice.emodb.sor.core.UpdateRef;
import com.bazaarvoice.emodb.web.jersey.params.SecondsParam;
import com.bazaarvoice.emodb.web.resources.SuccessResponse;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
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
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A back door into the raw event stream behind the Databus, for debugging.  This is intended for internal use only
 * and may change without warning.
 * <p>
 * This resource exposes methods similar to the generic queue service to send, peek, poll messages using the internal
 * table/key/changeId fields in a Databus event, bypassing the System of Record.
 * <p>
 * Note: there is no Java client for this API--it is REST only--so it takes shortcuts like defining its JSON
 * model objects inline.
 */
@Produces(MediaType.APPLICATION_JSON)
@RequiresPermissions ("system|raw_databus")
public class RawDatabusResource1 {

    public static class Update {
        public String table;
        public String key;
        public UUID changeId;
        public Set<String> tags;
    }

    public static class Event {
        public String id;
        public UpdateRef payload;  // Name matches the QueueService Message object.

        private Event(String id, UpdateRef payload) {
            this.id = id;
            this.payload = payload;
        }
    }

    private final DatabusEventStore _eventStore;

    public RawDatabusResource1(DatabusEventStore eventStore) {
        _eventStore = requireNonNull(eventStore, "eventStore");
    }

    @POST
    @Path("{subscription}/send")
    @Consumes(MediaType.APPLICATION_JSON)
    public SuccessResponse send(@PathParam("subscription") String subscription, Update update) {
        _eventStore.addAll(toInternal(ImmutableMap.of(subscription, ImmutableList.of(update))));
        return SuccessResponse.instance();
    }

    @POST
    @Path("{subscription}/sendbatch")
    @Consumes(MediaType.APPLICATION_JSON)
    public SuccessResponse sendBatch(@PathParam("subscription") String subscription, Collection<Update> updates) {
        _eventStore.addAll(toInternal(ImmutableMap.of(subscription, updates)));
        return SuccessResponse.instance();
    }

    @POST
    @Path("_sendbatch")
    @Consumes(MediaType.APPLICATION_JSON)
    public SuccessResponse sendBatches(Map<String, Collection<Update>> updatesBySubscription) {
        _eventStore.addAll(toInternal(updatesBySubscription));
        return SuccessResponse.instance();
    }

    @GET
    @Path("{subscription}/peek")
    public List<Event> peek(@PathParam("subscription") String subscription,
                            @QueryParam("limit") @DefaultValue("10") IntParam limit) {
        checkLegalSubscriptionName(subscription);
        return fromInternal(_eventStore.peek(subscription, limit.get()));
    }

    @GET
    @Path("{subscription}/poll")
    public List<Event> poll(@PathParam("subscription") String subscription,
                            @QueryParam("ttl") @DefaultValue("30") SecondsParam claimTtl,
                            @QueryParam("limit") @DefaultValue("10") IntParam limit) {
        checkLegalSubscriptionName(subscription);
        return fromInternal(_eventStore.poll(subscription, claimTtl.get(), limit.get()));
    }

    @POST
    @Path("{subscription}/renew")
    @Consumes(MediaType.APPLICATION_JSON)
    public SuccessResponse renew(@PathParam("subscription") String subscription,
                                 @QueryParam("ttl") @DefaultValue("30") SecondsParam claimTtl,
                                 List<String> messageIds) {
        checkLegalSubscriptionName(subscription);
        // Check for null parameters, which will throw a 400, otherwise it throws a 5xx error
        checkArgument(messageIds != null, "Missing message Ids");
        _eventStore.renew(subscription, messageIds, claimTtl.get(), true);
        return SuccessResponse.instance();
    }

    @POST
    @Path("{subscription}/ack")
    @Consumes(MediaType.APPLICATION_JSON)
    public SuccessResponse acknowledge(@PathParam("subscription") String subscription,
                                       List<String> messageIds) {
        checkLegalSubscriptionName(subscription);
        // Check for null parameters, which will throw a 400, otherwise it throws a 5xx error
        checkArgument(messageIds != null, "Missing message Ids");
        _eventStore.delete(subscription, messageIds, true);
        return SuccessResponse.instance();
    }

    private Multimap<String, ByteBuffer> toInternal(Map<String, ? extends Collection<Update>> map) {
        ImmutableMultimap.Builder<String, ByteBuffer> builder = ImmutableMultimap.builder();
        for (Map.Entry<String, ? extends Collection<Update>> entry : map.entrySet()) {
            String subscription = entry.getKey();
            Collection<Update> updates = entry.getValue();

            checkLegalSubscriptionName(subscription);

            for (Update update : updates) {
                checkArgument(update.table != null, "table is required");
                checkArgument(update.key != null, "key is required");
                checkArgument(update.changeId == null || update.changeId.version() == 1, "changeId must be a time uuid");

                // Similar to Databus.injectEvent(), provide a default value for changeId.
                UUID changeId = (update.changeId != null) ? update.changeId : TimeUUIDs.minimumUuid();

                UpdateRef ref = new UpdateRef(update.table, update.key, changeId, update.tags);
                builder.put(subscription, UpdateRefSerializer.toByteBuffer(ref));
            }
        }
        return builder.build();
    }

    private List<Event> fromInternal(List<EventData> events) {
        return Lists.transform(events, new Function<EventData, Event>() {
            @Override
            public Event apply(EventData event) {
                UpdateRef ref = UpdateRefSerializer.fromByteBuffer(event.getData());
                return new Event(event.getId(), ref);
            }
        });
    }

    private void checkLegalSubscriptionName(String subscription) {
        checkArgument(Names.isLegalSubscriptionName(subscription),
                "Subscription name must be a lowercase ASCII string between 1 and 255 characters in length. " +
                        "Allowed punctuation characters are -.:@_ and the subscription name may not start with a single underscore character. " +
                        "An example of a valid subscription name would be 'polloi:review'.");
    }
}
