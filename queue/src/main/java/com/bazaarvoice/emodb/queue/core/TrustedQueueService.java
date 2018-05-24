package com.bazaarvoice.emodb.queue.core;

import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.MoveQueueStatus;
import com.bazaarvoice.emodb.queue.api.QueueService;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of AuthQueueService that defers all calls to an {@link QueueService} which doesn't require authentication.
 * This class is useful when the user has already been authenticated and the calling method needs to forward control to
 * either a client on another server (and therefore requires authentication on that server) or an internal implementation
 * (which does not require authentication).
 */
public class TrustedQueueService implements AuthQueueService {

    private final QueueService _queueService;

    public TrustedQueueService(QueueService queueService) {
        _queueService = checkNotNull(queueService, "queueService");
    }

    @Override
    public void send(String apiKey, String queue, Object message) {
        _queueService.send(queue, message);
    }

    @Override
    public List<Message> poll(String apiKey, String queue, Duration claimTtl, int limit) {
        return _queueService.poll(queue, claimTtl, limit);
    }

    @Override
    public long getMessageCount(String apiKey, String queue) {
        return _queueService.getMessageCount(queue);
    }

    @Override
    public List<Message> peek(String apiKey, String queue, int limit) {
        return _queueService.peek(queue, limit);
    }

    @Override
    public void unclaimAll(String apiKey, String queue) {
        _queueService.unclaimAll(queue);
    }

    @Override
    public void sendAll(String apiKey, String queue, Collection<?> messages) {
        _queueService.sendAll(queue, messages);
    }

    @Override
    public long getClaimCount(String apiKey, String queue) {
        return _queueService.getClaimCount(queue);
    }

    @Override
    public String moveAsync(String apiKey, String from, String to) {
        return _queueService.moveAsync(from, to);
    }

    @Override
    public MoveQueueStatus getMoveStatus(String apiKey, String reference) {
        return _queueService.getMoveStatus(reference);
    }

    @Override
    public void renew(String apiKey, String queue, Collection<String> messageIds, Duration claimTtl) {
        _queueService.renew(queue, messageIds, claimTtl);
    }

    @Override
    public void acknowledge(String apiKey, String queue, Collection<String> messageIds) {
        _queueService.acknowledge(queue, messageIds);
    }

    @Override
    public long getMessageCountUpTo(String apiKey, String queue, long limit) {
        return _queueService.getMessageCountUpTo(queue, limit);
    }

    @Override
    public void purge(String apiKey, String queue) {
        _queueService.purge(queue);
    }

    @Override
    public void sendAll(String apiKey, Map<String, ? extends Collection<?>> messagesByQueue) {
        _queueService.sendAll(messagesByQueue);
    }
}
