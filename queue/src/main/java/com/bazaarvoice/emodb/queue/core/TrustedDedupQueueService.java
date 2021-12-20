package com.bazaarvoice.emodb.queue.core;

import com.bazaarvoice.emodb.queue.api.AuthDedupQueueService;
import com.bazaarvoice.emodb.queue.api.DedupQueueService;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.MoveQueueStatus;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of AuthDedupQueueService that defers all calls to a {@link DedupQueueService} which doesn't require authentication.
 * This class is useful when the user has already been authenticated and the calling method needs to forward control to
 * either a client on another server (and therefore requires authentication on that server) or an internal implementation
 * (which does not require authentication).
 */
public class TrustedDedupQueueService implements AuthDedupQueueService {

    private final DedupQueueService _dedupQueueService;

    public TrustedDedupQueueService(DedupQueueService dedupQueueService) {
        _dedupQueueService = requireNonNull(dedupQueueService, "dedupQueueService");
    }

    @Override
    public void send(String apiKey, String queue, Object message) {
        _dedupQueueService.send(queue, message);
    }

    @Override
    public List<Message> poll(String apiKey, String queue, Duration claimTtl, int limit) {
        return _dedupQueueService.poll(queue, claimTtl, limit);
    }

    @Override
    public long getMessageCount(String apiKey, String queue) {
        return _dedupQueueService.getMessageCount(queue);
    }

    @Override
    public List<Message> peek(String apiKey, String queue, int limit) {
        return _dedupQueueService.peek(queue, limit);
    }

    @Override
    public void unclaimAll(String apiKey, String queue) {
        _dedupQueueService.unclaimAll(queue);
    }

    @Override
    public void sendAll(String apiKey, String queue, Collection<?> messages) {
        _dedupQueueService.sendAll(queue, messages);
    }

    @Override
    public long getClaimCount(String apiKey, String queue) {
        return _dedupQueueService.getClaimCount(queue);
    }

    @Override
    public String moveAsync(String apiKey, String from, String to) {
        return _dedupQueueService.moveAsync(from, to);
    }

    @Override
    public MoveQueueStatus getMoveStatus(String apiKey, String reference) {
        return _dedupQueueService.getMoveStatus(reference);
    }

    @Override
    public void renew(String apiKey, String queue, Collection<String> messageIds, Duration claimTtl) {
        _dedupQueueService.renew(queue, messageIds, claimTtl);
    }

    @Override
    public void acknowledge(String apiKey, String queue, Collection<String> messageIds) {
        _dedupQueueService.acknowledge(queue, messageIds);
    }

    @Override
    public long getMessageCountUpTo(String apiKey, String queue, long limit) {
        return _dedupQueueService.getMessageCountUpTo(queue, limit);
    }

    @Override
    public void purge(String apiKey, String queue) {
        _dedupQueueService.purge(queue);
    }

    @Override
    public void sendAll(String apiKey, Map<String, ? extends Collection<?>> messagesByQueue) {
        _dedupQueueService.sendAll(messagesByQueue);
    }
}
