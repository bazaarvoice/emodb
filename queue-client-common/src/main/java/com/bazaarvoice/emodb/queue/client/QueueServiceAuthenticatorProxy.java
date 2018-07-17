package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.MoveQueueStatus;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.ostrich.partition.PartitionKey;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * QueueService instance that takes an {@link AuthQueueService} and API key and proxies all calls using
 * the API key.
 *
 * Note: The {@link PartitionKey} annotations must match those from AuthQueueService.
 */

class QueueServiceAuthenticatorProxy implements QueueService {

    private final AuthQueueService _authQueueService;
    private final String _apiKey;

    QueueServiceAuthenticatorProxy(AuthQueueService authQueueService, String apiKey) {
        _authQueueService = authQueueService;
        _apiKey = apiKey;
    }

    @Override
    public void send(String queue, Object message) {
        _authQueueService.send(_apiKey, queue, message);
    }

    @Override
    public void sendAll(Map<String, ? extends Collection<?>> messagesByQueue) {
        _authQueueService.sendAll(_apiKey, messagesByQueue);
    }

    @Override
    public MoveQueueStatus getMoveStatus(String reference) {
        return _authQueueService.getMoveStatus(_apiKey, reference);
    }

    @Override
    public long getClaimCount(@PartitionKey String queue) {
        return _authQueueService.getClaimCount(_apiKey, queue);
    }

    @Override
    public String moveAsync(String from, String to) {
        return _authQueueService.moveAsync(_apiKey, from, to);
    }

    @Override
    public void unclaimAll(@PartitionKey String queue) {
        _authQueueService.unclaimAll(_apiKey, queue);
    }

    @Override
    public void renew(@PartitionKey String queue, Collection<String> messageIds, Duration claimTtl) {
        _authQueueService.renew(_apiKey, queue, messageIds, claimTtl);
    }

    @Override
    public void sendAll(String queue, Collection<?> messages) {
        _authQueueService.sendAll(_apiKey, queue, messages);
    }

    @Override
    public void purge(@PartitionKey String queue) {
        _authQueueService.purge(_apiKey, queue);
    }

    @Override
    public long getMessageCount(String queue) {
        return _authQueueService.getMessageCount(_apiKey, queue);
    }

    @Override
    public List<Message> poll(@PartitionKey String queue, Duration claimTtl, int limit) {
        return _authQueueService.poll(_apiKey, queue, claimTtl, limit);
    }

    @Override
    public long getMessageCountUpTo(String queue, long limit) {
        return _authQueueService.getMessageCountUpTo(_apiKey, queue, limit);
    }

    @Override
    public List<Message> peek(String queue, int limit) {
        return _authQueueService.peek(_apiKey, queue, limit);
    }

    @Override
    public void acknowledge(@PartitionKey String queue, Collection<String> messageIds) {
        _authQueueService.acknowledge(_apiKey, queue, messageIds);
    }

    AuthQueueService getProxiedInstance() {
        return _authQueueService;
    }
}
