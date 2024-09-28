package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.auth.apikey.ApiKeyRequest;
import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.client.uri.EmoUriBuilder;
import com.bazaarvoice.emodb.common.api.Ttls;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.MoveQueueStatus;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.util.JSONWrappedObject;
import com.google.common.collect.Iterables;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;

import javax.ws.rs.core.MediaType;
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
    protected final RetryPolicy<Object> _retryPolicy;

    protected AbstractQueueClient(URI endPoint, boolean partitionSafe, EmoClient client, RetryPolicy<Object> retryPolicy) {
        _client = requireNonNull(client, "client");
        _queueService = EmoUriBuilder.fromUri(endPoint);
        _partitionSafe = partitionSafe;
        _retryPolicy = requireNonNull(retryPolicy);
    }

    public void send(String apiKey, String queue, final Object message) {
        requireNonNull(queue, "queue");
        URI uri = _queueService.clone()
                .segment(queue, "send")
                .build();

        Failsafe.with(_retryPolicy)
                .run(() -> _client.resource(uri)
                        .type(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        // Wrap the message in a JSONWrappedObject that forces Jersey to use Jackson to serialize it
                        // no matter what the type.  Otherwise null and String objects don't get serialized correctly.
                        .post(new JSONWrappedObject("", "", message)));
    }

    public void sendAll(String apiKey, String queue, Collection<?> messages) {
        requireNonNull(queue, "queue");
        requireNonNull(messages, "messages");
        if (messages.isEmpty()) {
            return;
        }
        URI uri = _queueService.clone()
                .segment(queue, "sendbatch")
                .build();

        Failsafe.with(_retryPolicy)
                .run(() -> _client.resource(uri)
                        .type(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .post(messages));
    }

    public void sendAll(String apiKey, String queue, Collection<?> messages, boolean isFlush) {
        requireNonNull(queue, "queue");
        requireNonNull(messages, "messages");
        if (messages.isEmpty()) {
            return;
        }
        URI uri = _queueService.clone()
                .segment(queue, "sendbatch")
                .build();

        Failsafe.with(_retryPolicy)
                .run(() -> _client.resource(uri)
                        .type(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .post(messages));
    }

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

        URI uri = _queueService.clone()
                .segment("_sendbatch")
                .build();
        Failsafe.with(_retryPolicy)
                .run(() -> _client.resource(uri)
                        .type(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .post(messagesByQueue));
    }

    protected long doGetMessageCountUpTo(String apiKey, String queue, long limit, boolean partitioned) {
        requireNonNull(queue, "queue");

        URI uri = _queueService.clone()
                .segment(queue, "size")
                .queryParam("limit", optional(limit != Long.MAX_VALUE ? limit : null))
                .queryParam("partitioned", optional(partitioned ? _partitionSafe : null))
                .build();
        return Failsafe.with(_retryPolicy)
                .get(() -> _client.resource(uri)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .get(Long.class));

    }

    protected long doGetClaimCount(String apiKey, String queue) {
        requireNonNull(queue, "queue");
        URI uri = _queueService.clone()
                .segment(queue, "claimcount")
                .queryParam("partitioned", _partitionSafe)
                .build();

        return Failsafe.with(_retryPolicy)
                .get(() -> _client.resource(uri)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .get(Long.class));
    }

    protected List<Message> doPeek(String apiKey, String queue, int limit, boolean partitioned) {
        requireNonNull(queue, "queue");

        URI uri = _queueService.clone()
                .segment(queue, "peek")
                .queryParam("limit", limit)
                .queryParam("partitioned", optional(partitioned ? _partitionSafe : null))
                .build();

        return Failsafe.with(_retryPolicy)
                .get(() -> _client.resource(uri)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .get(new TypeReference<List<Message>>() {
                        }));
    }

    protected List<Message> doPoll(String apiKey, String queue, Duration claimTtl, int limit) {
        requireNonNull(queue, "queue");
        requireNonNull(claimTtl, "claimTtl");
        URI uri = _queueService.clone()
                .segment(queue, "poll")
                .queryParam("ttl", Ttls.toSeconds(claimTtl, 0, Integer.MAX_VALUE))
                .queryParam("limit", limit)
                .queryParam("partitioned", _partitionSafe)
                .build();
        return Failsafe.with(_retryPolicy)
                .get(() -> _client.resource(uri)
                        .accept(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .get(new TypeReference<List<Message>>() {
                        }));
    }

    protected void doRenew(String apiKey, String queue, Collection<String> messageIds, Duration claimTtl) {
        requireNonNull(queue, "queue");
        requireNonNull(messageIds, "messageIds");
        requireNonNull(claimTtl, "claimTtl");

        URI uri = _queueService.clone()
                .segment(queue, "renew")
                .queryParam("ttl", Ttls.toSeconds(claimTtl, 0, Integer.MAX_VALUE))
                .queryParam("partitioned", _partitionSafe)
                .build();
        Failsafe.with(_retryPolicy)
                .run(() -> _client.resource(uri)
                        .type(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .post(messageIds));
    }

    protected void doAcknowledge(String apiKey, String queue, Collection<String> messageIds) {
        requireNonNull(queue, "queue");
        requireNonNull(messageIds, "messageIds");

        URI uri = _queueService.clone()
                .segment(queue, "ack")
                .queryParam("partitioned", _partitionSafe)
                .build();
        Failsafe.with(_retryPolicy)
                .run(() -> _client.resource(uri)
                        .type(MediaType.APPLICATION_JSON_TYPE)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .post(messageIds));
    }

    protected String doMoveAsync(String apiKey, String from, String to) {
        requireNonNull(from, "from");
        requireNonNull(to, "to");

        URI uri = _queueService.clone()
                .segment("_move")
                .queryParam("from", from)
                .queryParam("to", to)
                .build();
        Map<String, Object> response = Failsafe.with(_retryPolicy)
                .get(() -> _client.resource(uri)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .post(new TypeReference<Map<String, Object>>() {
                        }, null));
        return response.get("id").toString();
    }

    protected MoveQueueStatus doGetMoveStatus(String apiKey, String reference) {
        requireNonNull(reference, "reference");
        URI uri = _queueService.clone()
                .segment("_move")
                .segment(reference)
                .build();
        return Failsafe.with(_retryPolicy)
                .get(() -> _client.resource(uri)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .get(MoveQueueStatus.class));
    }

    protected void doUnclaimAll(String apiKey, String queue) {
        requireNonNull(queue, "queue");
        URI uri = _queueService.clone()
                .segment(queue, "unclaimall")
                .queryParam("partitioned", _partitionSafe)
                .build();
        Failsafe.with(_retryPolicy).run(() -> _client.resource(uri)
                .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                .post());
    }

    protected void doPurge(String apiKey, String queue) {
        requireNonNull(queue, "queue");
        URI uri = _queueService.clone()
                .segment(queue)
                .queryParam("partitioned", _partitionSafe)
                .build();
        Failsafe.with(_retryPolicy)
                .run(() -> _client.resource(uri)
                        .header(ApiKeyRequest.AUTHENTICATION_HEADER, apiKey)
                        .delete());
    }

    private Object[] optional(Object queryArg) {
        return (queryArg != null) ? new Object[]{queryArg} : new Object[0];
    }
}
