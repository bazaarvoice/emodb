package com.bazaarvoice.queue.client2;

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
 * DedupQueueService instance that takes an {@link AuthDedupQueueService} and API key and proxies all calls using
 * the API key.
 */

class DedupQueueServiceAuthenticatorProxy implements DedupQueueService {

    private final AuthDedupQueueService _authDedupQueueService;
    private final String _apiKey;

    DedupQueueServiceAuthenticatorProxy(AuthDedupQueueService authDedupQueueService, String apiKey) {
        _authDedupQueueService = requireNonNull(authDedupQueueService, "AuthDedupQueueService");
        _apiKey = requireNonNull(apiKey, "apiKey");
    }

    @Override
    public void send(String queue, Object message) {
        _authDedupQueueService.send(_apiKey, queue, message);
    }

    @Override
    public void sendAll(Map<String, ? extends Collection<?>> messagesByQueue) {
        _authDedupQueueService.sendAll(_apiKey, messagesByQueue);
    }

    @Override
    public MoveQueueStatus getMoveStatus(String reference) {
        return _authDedupQueueService.getMoveStatus(_apiKey, reference);
    }

    @Override
    public long getClaimCount( String queue) {
        return _authDedupQueueService.getClaimCount(_apiKey, queue);
    }

    @Override
    public String moveAsync(String from, String to) {
        return _authDedupQueueService.moveAsync(_apiKey, from, to);
    }

    @Override
    public void unclaimAll( String queue) {
        _authDedupQueueService.unclaimAll(_apiKey, queue);
    }

    @Override
    public void renew( String queue, Collection<String> messageIds, Duration claimTtl) {
        _authDedupQueueService.renew(_apiKey, queue, messageIds, claimTtl);
    }

    @Override
    public void sendAll(String queue, Collection<?> messages) {
        _authDedupQueueService.sendAll(_apiKey, queue, messages);
    }

    @Override
    public void purge( String queue) {
        _authDedupQueueService.purge(_apiKey, queue);
    }

    @Override
    public long getMessageCount( String queue) {
        return _authDedupQueueService.getMessageCount(_apiKey, queue);
    }

    @Override
    public List<Message> poll( String queue, Duration claimTtl, int limit) {
        return _authDedupQueueService.poll(_apiKey, queue, claimTtl, limit);
    }

    @Override
    public long getMessageCountUpTo( String queue, long limit) {
        return _authDedupQueueService.getMessageCountUpTo(_apiKey, queue, limit);
    }

    @Override
    public List<Message> peek( String queue, int limit) {
        return _authDedupQueueService.peek(_apiKey, queue, limit);
    }

    @Override
    public void acknowledge( String queue, Collection<String> messageIds) {
        _authDedupQueueService.acknowledge(_apiKey, queue, messageIds);
    }

    AuthDedupQueueService getProxiedInstance() {
        return _authDedupQueueService;
    }
}
