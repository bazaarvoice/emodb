package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.queue.api.AuthDedupQueueService;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.MoveQueueStatus;
import dev.failsafe.RetryPolicy;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.List;

/**
 * Dedup queue client implementation that routes API calls to the EmoDB service.  The actual HTTP communication
 * is managed by the {@link EmoClient} implementation to allow for flexible usage by variety of HTTP client
 * implementations, such as Jersey.
 */
public class DedupQueueClient extends AbstractQueueClient implements AuthDedupQueueService {

    public DedupQueueClient(URI endPoint, EmoClient client, RetryPolicy<Object> retryPolicy) {
        super(endPoint, false, client, retryPolicy);
    }

    // Compared to QueueClient, the DedupQueueClient requires partition-based routing for more methods.

    @Override
    public long getMessageCount(String apiKey, String queue) {
        return getMessageCountUpTo(apiKey, queue, Long.MAX_VALUE);
    }

    @Override
    public long getMessageCountUpTo(String apiKey, String queue, long limit) {
        return doGetMessageCountUpTo(apiKey, queue, limit, true);
    }

    @Override
    public long getClaimCount(String apiKey, String queue) {
        return doGetClaimCount(apiKey, queue);
    }

    @Override
    public List<Message> peek(String apiKey, String queue, int limit) {
        return doPeek(apiKey, queue, limit, true);
    }

    @Override
    public List<Message> poll(String apiKey, String queue, Duration claimTtl, int limit) {
        return doPoll(apiKey, queue, claimTtl, limit);
    }

    @Override
    public void renew(String apiKey, String queue, Collection<String> messageIds, Duration claimTtl) {
        doRenew(apiKey, queue, messageIds, claimTtl);
    }

    @Override
    public void acknowledge(String apiKey, String queue, Collection<String> messageIds) {
        doAcknowledge(apiKey, queue, messageIds);
    }

    @Override
    public String moveAsync(String apiKey, String from, String to) {
        return doMoveAsync(apiKey, from, to);
    }

    @Override
    public MoveQueueStatus getMoveStatus(String apiKey, String reference) {
        return doGetMoveStatus(apiKey, reference);
    }

    @Override
    public void unclaimAll(String apiKey, String queue) {
        doUnclaimAll(apiKey, queue);
    }

    @Override
    public void purge(String apiKey, String queue) {
        doPurge(apiKey, queue);
    }
}
