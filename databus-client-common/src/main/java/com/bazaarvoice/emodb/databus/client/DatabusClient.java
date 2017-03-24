package com.bazaarvoice.emodb.databus.client;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.auth.proxy.Credential;
import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.client.EmoResponse;
import com.bazaarvoice.emodb.client.uri.EmoUriBuilder;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.Ttls;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.common.json.RisonHelper;
import com.bazaarvoice.emodb.databus.api.AuthDatabus;
import com.bazaarvoice.emodb.databus.api.DatabusEventTracerSpec;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionRequest;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.databus.api.ReplaySubscriptionRequest;
import com.bazaarvoice.emodb.databus.api.ReplaySubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.api.UnauthorizedSubscriptionException;
import com.bazaarvoice.emodb.databus.api.UnknownMoveException;
import com.bazaarvoice.emodb.databus.api.UnknownReplayException;
import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.bazaarvoice.ostrich.partition.PartitionKey;
import com.fasterxml.jackson.core.type.TypeReference;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.joda.time.DateTimeConstants.SECONDS_PER_DAY;

/**
 * Databus client implementation that routes API calls to the EmoDB service.  The actual HTTP communication
 * is managed by the {@link EmoClient} implementation to allow for flexible usage by variety of HTTP client
 * implementations, such as Jersey.
 */
public class DatabusClient implements AuthDatabus {

    /** Must match the service name in the EmoService class. */
    /*package*/ static final String BASE_SERVICE_NAME = "emodb-bus-1";

    /** Must match the @Path annotation on the DatabusResource class. */
    public static final String SERVICE_PATH = "/bus/1";

    private static final MediaType JSON_CONDITION_MEDIA_TYPE = new MediaType("application", "x.json-condition");

    /** Poll header indicating whether the databus subscription is empty. */
    private static final String POLL_DATABUS_EMPTY_HEADER = "X-BV-Databus-Empty";

    private final EmoClient _client;
    private final UriBuilder _databus;
    private final boolean _partitionSafe;

    public DatabusClient(URI endPoint, EmoClient client) {
        this(endPoint, false, client);
    }


    public DatabusClient(URI endPoint, boolean partitionSafe, EmoClient client) {
        _client = checkNotNull(client, "client");
        _databus = EmoUriBuilder.fromUri(endPoint);
        _partitionSafe = partitionSafe;
    }

    @Override
    public Iterator<Subscription> listSubscriptions(String apiKey, @Nullable String fromSubscriptionExclusive, long limit) {
        checkArgument(limit > 0, "Limit must be >0");
        try {
            // Note: Buffers everything in memory and does not stream results one-by-one like DataStore does.
            URI uri = _databus.clone()
                    .queryParam("from", optional(fromSubscriptionExclusive))
                    .queryParam("limit", limit)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<Iterator<Subscription>>() {});
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    // Any server can manage subscriptions, no need for @PartitionKey
    @Override
    public void subscribe(String apiKey, String subscription, Condition tableFilter,
                          Duration subscriptionTtl, Duration eventTtl) {
        subscribe(apiKey, subscription, tableFilter, subscriptionTtl, eventTtl, true);
    }

    @Override
    public void subscribe(@Credential String apiKey, String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl, boolean includeDefaultJoinFilter) {
        checkNotNull(subscription, "subscription");
        checkNotNull(tableFilter, "tableFilter");
        try {
            URI uri = _databus.clone()
                    .segment(subscription)
                    .queryParam("ttl", Ttls.toSeconds(subscriptionTtl, 0, SECONDS_PER_DAY * 30))
                    .queryParam("eventTtl", Ttls.toSeconds(eventTtl, 0, SECONDS_PER_DAY * 30))
                    .queryParam("includeDefaultJoinFilter", Boolean.toString(includeDefaultJoinFilter))
                    .build();
            _client.resource(uri)
                    .type(JSON_CONDITION_MEDIA_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .put(tableFilter.toString());
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    // Any server can manage subscriptions, no need for @PartitionKey
    @Override
    public void unsubscribe(String apiKey, @PartitionKey String subscription) {
        checkNotNull(subscription, "subscription");
        try {
            URI uri = _databus.clone()
                    .segment(subscription)
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _client.resource(uri)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .delete();
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public Subscription getSubscription(String apiKey, String subscription) throws UnknownSubscriptionException {
        checkNotNull(subscription, "subscription");
        try {
            URI uri = _databus.clone()
                    .segment(subscription)
                    .build();
            return _client.resource(uri)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(Subscription.class);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public long getEventCount(String apiKey, @PartitionKey String subscription) {
        return getEventCountUpTo(apiKey, subscription, Long.MAX_VALUE);
    }

    @Override
    public long getEventCountUpTo(String apiKey, @PartitionKey String subscription, long limit) {
        checkNotNull(subscription, "subscription");
        try {
            URI uri = _databus.clone()
                    .segment(subscription, "size")
                    .queryParam("limit", optional(limit != Long.MAX_VALUE ? limit : null))
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(Long.class);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public long getClaimCount(String apiKey, @PartitionKey String subscription) {
        checkNotNull(subscription, "subscription");
        try {
            URI uri = _databus.clone()
                    .segment(subscription, "claimcount")
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(Long.class);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public Iterator<Event> peek(String apiKey, @PartitionKey String subscription, int limit) {
        checkNotNull(subscription, "subscription");
        try {
            URI uri = _databus.clone()
                    .segment(subscription, "peek")
                    .queryParam("limit", limit)
                    .queryParam("includeTags", "true")
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<Iterator<Event>>() {});
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public PollResult poll(String apiKey, @PartitionKey String subscription, Duration claimTtl, int limit) {
        checkNotNull(subscription, "subscription");
        checkNotNull(claimTtl, "claimTtl");

        URI uri = getPollUriBuilder(subscription, claimTtl, limit).build();
        EmoResponse response = _client.resource(uri)
                .queryParam("includeTags", "true")
                .accept(MediaType.APPLICATION_JSON_TYPE)
                .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                .get(EmoResponse.class);

        if (response.getStatus() != Response.Status.OK.getStatusCode()) {
            throw convertException(new EmoClientException(response));
        }

        Iterator<Event> events = response.getEntity(new TypeReference<Iterator<Event>>() {});

        boolean moreEvents;
        String databusEmpty = response.getFirstHeader(POLL_DATABUS_EMPTY_HEADER);
        if (databusEmpty != null) {
            // Use the header value from the server to determine if the databus subscription is empty
            moreEvents = !Boolean.parseBoolean(databusEmpty);
        } else {
            // Must be polling an older version of Emo which did not include this header.  Infer whether the queue
            // is empty based on whether any results were returned.
            moreEvents = events.hasNext();
        }

        return new PollResult(events, limit, moreEvents);
    }

    protected UriBuilder getPollUriBuilder(String subscription, Duration claimTtl, int limit) {
        return _databus.clone()
                .segment(subscription, "poll")
                .queryParam("ttl", Ttls.toSeconds(claimTtl, 0, Integer.MAX_VALUE))
                .queryParam("limit", limit)
                .queryParam("partitioned", _partitionSafe);
    }

    @Override
    public void renew(String apiKey, @PartitionKey String subscription, Collection<String> eventKeys, Duration claimTtl) {
        checkNotNull(subscription, "subscription");
        checkNotNull(eventKeys, "eventKeys");
        checkNotNull(claimTtl, "claimTtl");
        try {
            URI uri = _databus.clone()
                    .segment(subscription, "renew")
                    .queryParam("ttl", Ttls.toSeconds(claimTtl, 0, Integer.MAX_VALUE))
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post(eventKeys);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public void acknowledge(String apiKey, @PartitionKey String subscription, Collection<String> eventKeys) {
        checkNotNull(subscription, "subscription");
        checkNotNull(eventKeys, "eventKeys");
        try {
            URI uri = _databus.clone()
                    .segment(subscription, "ack")
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post(eventKeys);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    // Any server can initiate a replay request, no need for @PartitionKey
    @Override
    public String replayAsync(String apiKey, String subscription) {
        return replayAsyncSince(apiKey, subscription, null);
    }

    @Override
    public String replayAsyncSince(String apiKey, String subscription, Date since) {
        return replayAsync(apiKey,
                new ReplaySubscriptionRequest(subscription)
                        .since(since));
    }

    @Override
    public String replayAsync(@Credential String apiKey, ReplaySubscriptionRequest request) {
        checkNotNull(request, "request");
        String subscription = checkNotNull(request.getSubscription(), "subscription");
        Date since = request.getSince();
        DatabusEventTracerSpec tracer = request.getTracer();

        try {
            UriBuilder uriBuilder = _databus.clone().segment(subscription, "replay");
            if (since != null) {
                SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZ");
                dateFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
                uriBuilder.queryParam("since", dateFmt.format(since));
            }
            if (tracer != null) {
                uriBuilder.queryParam("tracer", RisonHelper.asORison(tracer));
            }
            URI uri = uriBuilder.build();
            Map<String, Object> response = _client.resource(uri)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post(new TypeReference<Map<String, Object>>(){}, null);
            return response.get("id").toString();
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    // Any server can get the replay status, no need for @PartitionKey
    @Override
    public ReplaySubscriptionStatus getReplayStatus(String apiKey, String reference) {
        checkNotNull(reference, "reference");
        try {
            URI uri = _databus.clone()
                    .segment("_replay")
                    .segment(reference)
                    .build();
            return _client.resource(uri)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(ReplaySubscriptionStatus.class);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    // Any server can initiate a move request, no need for @PartitionKey
    @Override
    public String moveAsync(String apiKey, String from, String to) {
        return moveAsync(apiKey, new MoveSubscriptionRequest(from, to));
    }

    @Override
    public String moveAsync(@Credential String apiKey, MoveSubscriptionRequest request) {
        checkNotNull(request, "request");
        String from = checkNotNull(request.getFrom(), "from");
        String to = checkNotNull(request.getTo(), "to");
        DatabusEventTracerSpec tracer = request.getTracer();
        try {
            UriBuilder uriBuilder = _databus.clone()
                    .segment("_move")
                    .queryParam("from", from)
                    .queryParam("to", to);
            if (tracer != null) {
                uriBuilder.queryParam("tracer", RisonHelper.asORison(tracer));
            }
            URI uri = uriBuilder.build();
            Map<String, Object> response = _client.resource(uri)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post(new TypeReference<Map<String, Object>>(){}, null);
            return response.get("id").toString();
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    // Any server can get the move status, no need for @PartitionKey
    @Override
    public MoveSubscriptionStatus getMoveStatus(String apiKey, String reference) {
        checkNotNull(reference, "reference");
        try {
            URI uri = _databus.clone()
                    .segment("_move")
                    .segment(reference)
                    .build();
            return _client.resource(uri)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(MoveSubscriptionStatus.class);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public void injectEvent(String apiKey, String subscription, String table, String key) {
        checkNotNull(subscription, "subscription");
        checkNotNull(table, "table");
        checkNotNull(key, "key");
        try {
            URI uri = _databus.clone()
                    .segment(subscription, "inject")
                    .queryParam("table", table)
                    .queryParam("key", key)
                    .build();
            _client.resource(uri)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post();
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public void unclaimAll(String apiKey, @PartitionKey String subscription) {
        checkNotNull(subscription, "subscription");
        try {
            URI uri = _databus.clone()
                    .segment(subscription, "unclaimall")
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _client.resource(uri)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post();
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @Override
    public void purge(String apiKey, @PartitionKey String subscription) {
        checkNotNull(subscription, "subscription");
        try {
            URI uri = _databus.clone()
                    .segment(subscription, "purge")
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _client.resource(uri)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post();
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    @SuppressWarnings ("ThrowableResultOfMethodCallIgnored")
    private RuntimeException convertException(EmoClientException e) {
        EmoResponse response = e.getResponse();
        String exceptionType = response.getFirstHeader("X-BV-Exception");

        if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode() &&
                IllegalArgumentException.class.getName().equals(exceptionType)) {
            return new IllegalArgumentException(response.getEntity(String.class), e);

        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                UnknownSubscriptionException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(UnknownSubscriptionException.class).initCause(e);
            } else {
                return (RuntimeException) new UnknownSubscriptionException().initCause(e);
            }

        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                UnknownMoveException.class.getName().equals(exceptionType)) {
            return response.getEntity(UnknownMoveException.class);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                UnknownReplayException.class.getName().equals(exceptionType)) {
            return response.getEntity(UnknownReplayException.class);
        } else if (response.getStatus() == Response.Status.FORBIDDEN.getStatusCode() &&
                UnauthorizedSubscriptionException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(UnauthorizedSubscriptionException.class).initCause(e);
            } else {
                return (RuntimeException) new UnauthorizedSubscriptionException().initCause(e);
            }
        } else if (response.getStatus() == Response.Status.FORBIDDEN.getStatusCode() &&
                UnauthorizedException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(UnauthorizedException.class).initCause(e);
            } else {
                return (RuntimeException) new UnauthorizedException().initCause(e);
            }
        } else if (response.getStatus() == Response.Status.SERVICE_UNAVAILABLE.getStatusCode() &&
                ServiceUnavailableException.class.getName().equals(exceptionType)) {
            if (response.hasEntity()) {
                return (RuntimeException) response.getEntity(ServiceUnavailableException.class).initCause(e);
            } else {
                return (RuntimeException) new ServiceUnavailableException().initCause(e);
            }
        }

        return e;
    }

    private Object[] optional(Object queryArg) {
        return (queryArg != null) ? new Object[]{queryArg} : new Object[0];
    }
}
