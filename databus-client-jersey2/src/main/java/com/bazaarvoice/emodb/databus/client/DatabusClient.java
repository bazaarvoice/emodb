package com.bazaarvoice.emodb.databus.client;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.client.EmoResponse;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.Ttls;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.databus.api.Databus;
import com.bazaarvoice.emodb.databus.api.Event;
import com.bazaarvoice.emodb.databus.api.MoveSubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.PollResult;
import com.bazaarvoice.emodb.databus.api.ReplaySubscriptionStatus;
import com.bazaarvoice.emodb.databus.api.Subscription;
import com.bazaarvoice.emodb.databus.api.UnauthorizedSubscriptionException;
import com.bazaarvoice.emodb.databus.api.UnknownMoveException;
import com.bazaarvoice.emodb.databus.api.UnknownReplayException;
import com.bazaarvoice.emodb.databus.api.UnknownSubscriptionException;
import com.bazaarvoice.emodb.databus.client.discovery.EmoServiceDiscovery;
import com.bazaarvoice.emodb.sor.condition.Condition;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.Closeable;
import java.net.URI;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;


/**
 * Thin client for accessing EmoDB Databus.
 */
public class DatabusClient implements Databus, Closeable {

    private final static Logger _log = LoggerFactory.getLogger(DatabusClient.class);

    private static final String POLL_DATABUS_EMPTY_HEADER = "X-BV-Databus-Empty";
    private static final MediaType JSON_CONDITION_MEDIA_TYPE = new MediaType("application", "x.json-condition");

    private EmoServiceDiscovery _databusDiscovery;
    private final EmoClient _client;
    private final String _apiKey;
    private final RetryPolicy<Object> _retryPolicy;
    private static final boolean _partitionSafe = false;

    public DatabusClient(EmoServiceDiscovery databusDiscovery, EmoClient client, String apiKey, RetryPolicy<Object> retryPolicy) {
        _databusDiscovery = databusDiscovery;
        _client = client;
        _apiKey = apiKey;
        _retryPolicy = requireNonNull(retryPolicy);
    }

    @Override
    public Iterator<Subscription> listSubscriptions(@Nullable String fromSubscriptionExclusive, long limit) {
        checkArgument(limit > 0, "Limit must be >0");
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .queryParam("from", optional(fromSubscriptionExclusive))
                    .queryParam("limit", limit)
                    .build();
            _log.debug("Uri for listSubscriptions call:{} ", uri.toString());

            return Failsafe.with(_retryPolicy)
                    .get(() -> _client.resource(uri)
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .get(new TypeReference<Iterator<Subscription>>() {
                            }));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void subscribe(String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl) {
        subscribe(subscription, tableFilter, subscriptionTtl, eventTtl, true);

    }

    @Override
    public void subscribe(String subscription, Condition tableFilter, Duration subscriptionTtl, Duration eventTtl, boolean includeDefaultJoinFilter) {
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path(subscription)
                    .queryParam("ttl", subscriptionTtl.getSeconds())
                    .queryParam("eventTtl", eventTtl.getSeconds())
                    .queryParam("includeDefaultJoinFilter", includeDefaultJoinFilter)
                    .build();
            _log.debug("Uri for subscribe call:{} ", uri.toString());

            Failsafe.with(_retryPolicy)
                    .run(() -> _client.resource(uri)
                            .type(JSON_CONDITION_MEDIA_TYPE)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .put(tableFilter.toString()));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unsubscribe(String subscription) {
        requireNonNull(subscription, "subscription");
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path(subscription)
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _log.debug("Uri for unsubscribe call:{} ", uri.toString());

            Failsafe.with(_retryPolicy)
                    .run(() -> _client.resource(uri)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .delete());
        }  catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Subscription getSubscription(String subscription) throws UnknownSubscriptionException {
        requireNonNull(subscription, "subscription");
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path(subscription)
                    .build();
            _log.debug("Uri for getSubscription call:{} ", uri.toString());

            return Failsafe.with(_retryPolicy)
                    .get(() -> _client.resource(uri)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .get(Subscription.class));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getEventCount(String subscription) {
        return getEventCountUpTo(subscription, Long.MAX_VALUE);
    }

    @Override
    public long getEventCountUpTo(String subscription, long limit) {
        requireNonNull(subscription, "subscription");
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path(subscription)
                    .path("size")
                    .queryParam("limit", optional(limit != Long.MAX_VALUE ? limit : null))
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _log.debug("Uri for getEventCountUpTo call:{} ", uri.toString());
            return Failsafe.with(_retryPolicy)
                    .get(() -> _client.resource(uri)
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .get(Long.class));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long getClaimCount(String subscription) {
        requireNonNull(subscription, "subscription");
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path(subscription)
                    .path("claimcount")
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _log.debug("Uri for getClaimCount call:{} ", uri.toString());

            return Failsafe.with(_retryPolicy)
                    .get(() -> _client.resource(uri)
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .get(Long.class));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Event> peek(String subscription, int limit) {
        requireNonNull(subscription, "subscription");
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path(subscription)
                    .path("peek")
                    .queryParam("limit", limit)
                    .queryParam("includeTags", "true")
                    .build();
            _log.debug("Uri for peek call:{} ", uri.toString());

            return Failsafe.with(_retryPolicy)
                    .get(() -> _client.resource(uri)
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .get(new TypeReference<Iterator<Event>>() {
                            }));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PollResult poll(String subscription, Duration claimTtl, int limit) {
        requireNonNull(subscription, "subscription");
        requireNonNull(claimTtl, "claimTtl");

        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path(subscription)
                    .path("poll")
                    .queryParam("ttl", Ttls.toSeconds(claimTtl, 0, Integer.MAX_VALUE))
                    .queryParam("limit", limit)
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _log.debug("Uri for poll call:{} ", uri.toString());

            EmoResponse response = Failsafe.with(_retryPolicy)
                    .get(() -> _client.resource(uri)
                            .queryParam("includeTags", "true")
                            .accept(MediaType.APPLICATION_JSON_TYPE)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .get(EmoResponse.class));

            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new EmoClientException(response);
            }

            Iterator<Event> events = response.getEntity(new TypeReference<Iterator<Event>>() {
            });

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

        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void renew(String subscription, Collection<String> eventKeys, Duration claimTtl) {
        requireNonNull(subscription, "subscription");
        requireNonNull(eventKeys, "eventKeys");
        requireNonNull(claimTtl, "claimTtl");
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path(subscription)
                    .path("renew")
                    .queryParam("ttl", Ttls.toSeconds(claimTtl, 0, Integer.MAX_VALUE))
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _log.debug("Uri for renew call:{} ", uri.toString());

            Failsafe.with(_retryPolicy)
                    .run(() -> _client.resource(uri)
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .post(eventKeys));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void acknowledge(String subscription, Collection<String> eventKeys) {
        requireNonNull(subscription, "subscription");
        requireNonNull(eventKeys, "eventKeys");
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path(subscription)
                    .path("ack")
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _log.info("Uri for acknowledge call:{} ", uri.toString());
            Failsafe.with(_retryPolicy)
                    .run(() -> _client.resource(uri)
                            .type(MediaType.APPLICATION_JSON_TYPE)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .post(eventKeys));
            _log.info("Uri:{} ", uri.toString());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            _log.error("Error occured from Acknowledge ",e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public String replayAsync(String subscription) {
        return replayAsyncSince(subscription, null);
    }

    @Override
    public String replayAsyncSince(String subscription, Date since) {
        requireNonNull(subscription, "subscription");
        try {
            UriBuilder uriBuilder = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path(subscription)
                    .path("replay");
            if (since != null) {
                SimpleDateFormat dateFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZ");
                dateFmt.setTimeZone(TimeZone.getTimeZone("UTC"));
                uriBuilder.queryParam("since", dateFmt.format(since));
            }
            URI uri = uriBuilder.build();
            _log.debug("Uri for replayAsyncSince call:{} ", uri.toString());

            Map<String, Object> response = Failsafe.with(_retryPolicy)
                    .get(() -> _client.resource(uri)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .post(new TypeReference<Map<String, Object>>() {
                            }, null));
            return response.get("id").toString();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public ReplaySubscriptionStatus getReplayStatus(String reference) {
        requireNonNull(reference, "reference");
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path("replay")
                    .path(reference)
                    .build();
            _log.debug("Uri for getReplayStatus call:{} ", uri.toString());

            return Failsafe.with(_retryPolicy)
                    .get(() -> _client.resource(uri)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .get(ReplaySubscriptionStatus.class));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String moveAsync(String from, String to) {
        requireNonNull(from, "from");
        requireNonNull(to, "to");
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path("_move")
                    .queryParam("from", from)
                    .queryParam("to", to)
                    .build();
            _log.debug("Uri for moveAsync call:{} ", uri.toString());

            Map<String, Object> response = Failsafe.with(_retryPolicy)
                    .get(() -> _client.resource(uri)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .post(new TypeReference<Map<String, Object>>() {
                            }, null));
            return response.get("id").toString();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MoveSubscriptionStatus getMoveStatus(String reference) {
        requireNonNull(reference, "reference");
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path("_move")
                    .path(reference)
                    .build();
            _log.debug("Uri for getMoveStatus call:{} ", uri.toString());

            return Failsafe.with(_retryPolicy)
                    .get(() -> _client.resource(uri)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .get(MoveSubscriptionStatus.class));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void injectEvent(String subscription, String table, String key) {
        requireNonNull(subscription, "subscription");
        requireNonNull(table, "table");
        requireNonNull(key, "key");
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path(subscription)
                    .path("inject")
                    .queryParam("table", table)
                    .queryParam("key", key)
                    .build();
            _log.debug("Uri for injectEvent call:{} ", uri.toString());

            Failsafe.with(_retryPolicy)
                    .run(() -> _client.resource(uri)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .post(null));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unclaimAll(String subscription) {
        requireNonNull(subscription, "subscription");
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path(subscription)
                    .path("unclaimall")
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _log.debug("Uri for unclaimAll call:{} ", uri.toString());

            Failsafe.with(_retryPolicy)
                    .run(() -> _client.resource(uri)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .post(null));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void purge(String subscription) {
        requireNonNull(subscription, "subscription");
        try {
            URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                    .path(subscription)
                    .path("purge")
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _log.debug("Uri for purge call:{} ", uri.toString());

            Failsafe.with(_retryPolicy)
                    .run(() -> _client.resource(uri)
                            .header(ApiKeyRequest.AUTHENTICATION_HEADER, _apiKey)
                            .post());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    private Object[] optional(Object queryArg) {
        return (queryArg != null) ? new Object[]{queryArg} : new Object[0];
    }


    @Override
    synchronized public void close() {
        _log.debug("Closing ServiceDiscovery... ");
//        TODO think how to improve this
        _databusDiscovery = null;
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }
}
