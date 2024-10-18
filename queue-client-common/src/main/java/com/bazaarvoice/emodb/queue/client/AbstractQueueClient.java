package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.EmoClientException;
import com.bazaarvoice.emodb.client.EmoResponse;
import com.bazaarvoice.emodb.client.uri.EmoUriBuilder;
import com.bazaarvoice.emodb.common.api.ServiceUnavailableException;
import com.bazaarvoice.emodb.common.api.Ttls;
import com.bazaarvoice.emodb.common.api.UnauthorizedException;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.MoveQueueStatus;
import com.bazaarvoice.emodb.queue.api.UnknownMoveException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.util.JSONWrappedObject;
import com.google.common.collect.Iterables;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

abstract class AbstractQueueClient {
    protected final EmoClient _client;
    protected final UriBuilder _queueService;
    protected final boolean _partitionSafe;

    protected AbstractQueueClient(URI endPoint, boolean partitionSafe, EmoClient client) {
        _client = requireNonNull(client, "client");
        _queueService = EmoUriBuilder.fromUri(endPoint);
        _partitionSafe = partitionSafe;
    }

    // Any server can handle sending messages, no need for @PartitionKey
    public void send(String apiKey, String queue, final Object message) {
        requireNonNull(queue, "queue");
        try {
            URI uri = _queueService.clone()
                    .segment(queue, "send")
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    // Wrap the message in a JSONWrappedObject that forces Jersey to use Jackson to serialize it
                    // no matter what the type.  Otherwise null and String objects don't get serialized correctly.
                    .post(new JSONWrappedObject("", "", message));
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    // Any server can handle sending messages, no need for @PartitionKey
    public void sendAll(String apiKey, String queue, Collection<?> messages) {
        requireNonNull(queue, "queue");
        requireNonNull(messages, "messages");
        if (messages.isEmpty()) {
            return;
        }
        try {
            URI uri = _queueService.clone()
                    .segment(queue, "sendbatch")
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post(messages);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    public void sendAll(String apiKey, String queue, Collection<?> messages, boolean isFlush) {
        requireNonNull(queue, "queue");
        requireNonNull(messages, "messages");
        if (messages.isEmpty()) {
            return;
        }
        try {
            URI uri = _queueService.clone()
                    .segment(queue, "sendbatch")
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post(messages);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    // Any server can handle sending messages, no need for @PartitionKey
    public void sendAll(String apiKey, Map<String, ? extends Collection<?>> messagesByQueue) {
        requireNonNull(messagesByQueue, "messagesByQueue");
        if (messagesByQueue.isEmpty()) {
            return;
        }
        if (messagesByQueue.size() == 1) {
            // Prefer the single queue REST call because it'll log the queue name in the HTTP server request logs.
            Map.Entry<String, ? extends Collection<?>> entry = Iterables.getOnlyElement(messagesByQueue.entrySet());
            sendAll(apiKey, entry.getKey(), entry.getValue());
            return;
        }
        try {
            URI uri = _queueService.clone()
                    .segment("_sendbatch")
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post(messagesByQueue);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    protected long doGetMessageCountUpTo(String apiKey, String queue, long limit, boolean partitioned) {
        requireNonNull(queue, "queue");
        try {
            URI uri = _queueService.clone()
                    .segment(queue, "size")
                    .queryParam("limit", optional(limit != Long.MAX_VALUE ? limit : null))
                    .queryParam("partitioned", optional(partitioned ? _partitionSafe : null))
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(Long.class);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    protected long doGetClaimCount(String apiKey, String queue) {
        requireNonNull(queue, "queue");
        try {
            URI uri = _queueService.clone()
                    .segment(queue, "claimcount")
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

    protected List<Message> doPeek(String apiKey, String queue, int limit, boolean partitioned) {
        requireNonNull(queue, "queue");
        try {
            URI uri = _queueService.clone()
                    .segment(queue, "peek")
                    .queryParam("limit", limit)
                    .queryParam("partitioned", optional(partitioned ? _partitionSafe : null))
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<List<Message>>() {
                    });
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    protected List<Message> doPoll(String apiKey, String queue, Duration claimTtl, int limit) {
        requireNonNull(queue, "queue");
        requireNonNull(claimTtl, "claimTtl");
        try {
            URI uri = _queueService.clone()
                    .segment(queue, "poll")
                    .queryParam("ttl", Ttls.toSeconds(claimTtl, 0, Integer.MAX_VALUE))
                    .queryParam("limit", limit)
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            return _client.resource(uri)
                    .accept(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(new TypeReference<List<Message>>() {
                    });
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    protected void doRenew(String apiKey, String queue, Collection<String> messageIds, Duration claimTtl) {
        requireNonNull(queue, "queue");
        requireNonNull(messageIds, "messageIds");
        requireNonNull(claimTtl, "claimTtl");
        try {
            URI uri = _queueService.clone()
                    .segment(queue, "renew")
                    .queryParam("ttl", Ttls.toSeconds(claimTtl, 0, Integer.MAX_VALUE))
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post(messageIds);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    protected void doAcknowledge(String apiKey, String queue, Collection<String> messageIds) {
        requireNonNull(queue, "queue");
        requireNonNull(messageIds, "messageIds");
        try {
            URI uri = _queueService.clone()
                    .segment(queue, "ack")
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _client.resource(uri)
                    .type(MediaType.APPLICATION_JSON_TYPE)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post(messageIds);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    protected String doMoveAsync(String apiKey, String from, String to) {
        requireNonNull(from, "from");
        requireNonNull(to, "to");
        try {
            URI uri = _queueService.clone()
                    .segment("_move")
                    .queryParam("from", from)
                    .queryParam("to", to)
                    .build();
            Map<String, Object> response = _client.resource(uri)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post(new TypeReference<Map<String, Object>>() {
                    }, null);
            return response.get("id").toString();
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    protected MoveQueueStatus doGetMoveStatus(String apiKey, String reference) {
        requireNonNull(reference, "reference");
        try {
            URI uri = _queueService.clone()
                    .segment("_move")
                    .segment(reference)
                    .build();
            return _client.resource(uri)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .get(MoveQueueStatus.class);
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    protected void doUnclaimAll(String apiKey, String queue) {
        requireNonNull(queue, "queue");
        try {
            URI uri = _queueService.clone()
                    .segment(queue, "unclaimall")
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _client.resource(uri)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .post();
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    protected void doPurge(String apiKey, String queue) {
        requireNonNull(queue, "queue");
        try {
            URI uri = _queueService.clone()
                    .segment(queue)
                    .queryParam("partitioned", _partitionSafe)
                    .build();
            _client.resource(uri)
                    .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                    .delete();
        } catch (EmoClientException e) {
            throw convertException(e);
        }
    }

    protected RuntimeException convertException(EmoClientException e) {
        EmoResponse response = e.getResponse();
        String exceptionType = response.getFirstHeader("X-BV-Exception");

        if (response.getStatus() == Response.Status.BAD_REQUEST.getStatusCode() &&
                IllegalArgumentException.class.getName().equals(exceptionType)) {
            return new IllegalArgumentException(response.getEntity(String.class), e);
        } else if (response.getStatus() == Response.Status.NOT_FOUND.getStatusCode() &&
                UnknownMoveException.class.getName().equals(exceptionType)) {
            return response.getEntity(UnknownMoveException.class);
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
