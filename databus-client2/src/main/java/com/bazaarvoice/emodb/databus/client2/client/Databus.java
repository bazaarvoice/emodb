package com.bazaarvoice.emodb.databus.client2.client;


import com.bazaarvoice.emodb.databus.client2.exceptions.PollFailedException;
import com.bazaarvoice.emodb.databus.client2.exceptions.SubscribeFailedException;
import com.bazaarvoice.emodb.databus.client2.discovery.DatabusDiscovery;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.client.JerseyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static com.bazaarvoice.emodb.databus.client2.Json.JsonUtil.parseJson;
import static com.bazaarvoice.emodb.databus.client2.Json.JsonUtil.toJsonString;


/**
 * Thin client for accessing EmoDB Databus.
 */
public class Databus implements Closeable {

    private final static Logger _log = LoggerFactory.getLogger(Databus.class);

    private final DatabusDiscovery _databusDiscovery;
    private final JerseyClient _client;
    private final String _apiKey;
    private final DatabusClient _databusClient;

    public Databus(DatabusDiscovery databusDiscovery, JerseyClient client, String apiKey) {
        _databusDiscovery = databusDiscovery;
        _client = client;
        _apiKey = apiKey;
        _databusClient = new DatabusClient();
    }

    /**
     * Interface {@link IDatabusClient} is only to satisfy
     * use of {@link RetryProxy} for retrying low-level client API calls.
     */
    private interface IDatabusClient {
        void subscribe(String subscription, String condition, Duration subscriptionTtl, Duration eventTtl) throws SubscribeFailedException;

        PollResponse poll(String subscription, int limit, Duration ttl) throws PollFailedException;

        PollResponse peek(String subscription, int limit) throws PollFailedException;

        void acknowledge(String subscription, List<String> eventKeys) throws PollFailedException;

        long size(String subscription, Optional<Long> limit) throws PollFailedException;
    }

    private class DatabusClient implements IDatabusClient {
        @Override
        public void subscribe(String subscription, String condition, Duration subscriptionTtl, Duration eventTtl)
                throws SubscribeFailedException {
            Response response = null;
            try {
                URI uri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                        .path("bus")
                        .path("1")
                        .path(subscription)
                        .queryParam("ttl", subscriptionTtl.getSeconds())
                        .queryParam("eventTtl", eventTtl.getSeconds())
                        .queryParam("includeDefaultJoinFilter", "false")
                        .build();

                response = _client.target(uri).request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header("X-BV-API-Key", _apiKey)
                        .put(Entity.entity(condition, "application/x.json-condition"));

                if (response.getStatus() != HttpStatus.SC_OK) {
                    throw new SubscribeFailedException(subscription, response.getStatus(), response.readEntity(String.class));
                }
            } catch (SubscribeFailedException e) {
                throw e;
            } catch (Exception e) {
                throw new SubscribeFailedException(subscription, e);
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        }

        @Override
        public PollResponse poll(String subscription, int limit, Duration ttl) throws PollFailedException {
            return peekOrPoll(subscription, limit, ttl);
        }

        @Override
        public PollResponse peek(String subscription, int limit) throws PollFailedException {
            return peekOrPoll(subscription, limit, null);
        }

        private PollResponse peekOrPoll(String subscription, int limit, @Nullable Duration ttl) {
            Response response = null;
            try {
                UriBuilder uriBuilder = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                        .path("bus")
                        .path("1")
                        .path(subscription)
                        .path(ttl != null ? "poll" : "peek")
                        .queryParam("limit", limit);

                if (ttl != null) {
                    uriBuilder = uriBuilder.queryParam("ttl", ttl.getSeconds());
                }

                response = _client.target(uriBuilder.build()).request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header("X-BV-API-Key", _apiKey)
                        .get();

                if (response.getStatus() != HttpStatus.SC_OK) {
                    throw new PollFailedException(subscription, response.getStatus(), response.readEntity(String.class));
                }

                boolean subscriptionEmpty = false;
                String emptyHeader = response.getHeaderString("X-BV-Databus-Empty");
                if (emptyHeader != null) {
                    subscriptionEmpty = Boolean.parseBoolean(emptyHeader);
                }

                InputStream entity = response.readEntity(InputStream.class);
                List<PollResponse.Event> events = parseJson(entity, new TypeReference<List<PollResponse.Event>>() {});

                return new PollResponse(events, subscriptionEmpty);
            } catch (PollFailedException e) {
                throw e;
            } catch (Exception e) {
                throw new PollFailedException(subscription, e);
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        }

        @Override
        public void acknowledge(String subscription, List<String> eventKeys) throws PollFailedException {
            Response response = null;
            try {
                URI ackUri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                        .path("bus")
                        .path("1")
                        .path(subscription)
                        .path("ack")
                        .build();

                response = _client.target(ackUri).request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header("X-BV-API-Key", _apiKey)
                        .post(Entity.json(toJsonString(eventKeys)));

                if (response.getStatus() != HttpStatus.SC_OK) {
                    _log.warn("Events ack failed with response: code={}, entity={}", response.getStatus(), response.readEntity(String.class));
                    // Not really a failure in polling, but just need to raise some exception to force retries
                    throw new PollFailedException(subscription, response.getStatus(), response.readEntity(String.class));
                }
            } catch (PollFailedException e) {
                throw e;
            } catch (Exception e) {
                throw new PollFailedException(subscription, e);
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        }

        @Override
        public long size(String subscription, Optional<Long> limit) throws PollFailedException {
            Response response = null;
            try {
                URI sizeUri = UriBuilder.fromUri(_databusDiscovery.getBaseUri())
                        .path("bus")
                        .path("1")
                        .path(subscription)
                        .path("size")
                        .queryParam("limit", String.valueOf(limit.orElse(Long.MAX_VALUE)))
                        .build();

                response = _client.target(sizeUri).request()
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header("X-BV-API-Key", _apiKey)
                        .get();

                if (response.getStatus() != HttpStatus.SC_OK) {
                    _log.warn("Subscription size query failed with response: code={}, entity={}", response.getStatus(), response.readEntity(String.class));
                    // Not really a failure in polling, but just need to raise some exception to force retries
                    throw new PollFailedException(subscription, response.getStatus(), response.readEntity(String.class));
                }

                return parseJson(response.readEntity(InputStream.class), Long.class);
            } catch (PollFailedException e) {
                throw e;
            } catch (Exception e) {
                throw new PollFailedException(subscription, e);
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        }
    }

    public void subscribe(String subscription, String condition, Duration subscriptionTtl, Duration eventTtl) throws SubscribeFailedException {
        subscribe(subscription, condition, subscriptionTtl, eventTtl, null);
    }

    public void subscribe(String subscription, String condition, Duration subscriptionTtl, Duration eventTtl, @Nullable RetryPolicy retryPolicy)
            throws SubscribeFailedException {
        client(retryPolicy).subscribe(subscription, condition, subscriptionTtl, eventTtl);
    }

    public PollResponse poll(String subscription, int limit, Duration ttl) throws PollFailedException {
        return poll(subscription, limit, ttl, null);
    }

    public PollResponse poll(String subscription, int limit, Duration ttl, @Nullable RetryPolicy retryPolicy) throws PollFailedException {
        return client(retryPolicy).poll(subscription, limit, ttl);
    }

    public PollResponse peek(String subscription, int limit) throws PollFailedException {
        return peek(subscription, limit, null);
    }

    public PollResponse peek(String subscription, int limit, @Nullable RetryPolicy retryPolicy) throws PollFailedException {
        return client(retryPolicy).peek(subscription, limit);
    }

    public void acknowledge(String subscription, List<String> eventKeys) {
        acknowledge(subscription, eventKeys, null);
    }

    public void acknowledge(String subscription, List<String> eventKeys, @Nullable RetryPolicy retryPolicy) {
        try {
            client(retryPolicy).acknowledge(subscription, eventKeys);
        } catch (PollFailedException e) {
            // There's nothing the caller can do if the ack failed, even after completing all retries.  Additionally,
            // the databus will eventually redeliver the events; we'll get another crack at acking them then.
            _log.warn("Failed to acknowledge databus events", e.getCause());
        }
    }

    public long size(String subscription, Optional<Long> limit) throws PollFailedException {
        return size(subscription, limit, null);
    }

    public long size(String subscription, Optional<Long> limit, @Nullable RetryPolicy retryPolicy) throws PollFailedException {
        return client(retryPolicy).size(subscription, limit);
    }

    private IDatabusClient client(RetryPolicy retryPolicy) {
        if (retryPolicy == null || retryPolicy == RetryPolicies.TRY_ONCE_THEN_FAIL) {
            return _databusClient;
        } else {
            return (IDatabusClient) RetryProxy.create(IDatabusClient.class, _databusClient, retryPolicy);
        }
    }

    @Override
    synchronized public void close() throws IOException {
        _databusDiscovery.stop();// Check if stopAsync can be used here
        _client.close();
    }

    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }
}
