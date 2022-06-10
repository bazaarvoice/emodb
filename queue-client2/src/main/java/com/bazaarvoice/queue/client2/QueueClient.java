package com.bazaarvoice.queue.client2;

import com.bazaarvoice.emodb.client2.EmoClient;
import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.MoveQueueStatus;
import dev.failsafe.RetryPolicy;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.List;

/**
 * Queue client implementation that routes API calls to the EmoDB service.  The actual HTTP communication
 * is managed by the {@link EmoClient} implementation to allow for flexible usage by variety of HTTP client
 * implementations, such as Jersey.
 */
public class QueueClient extends AbstractQueueClient implements AuthQueueService {

    public QueueClient(URI endPoint, EmoClient client, RetryPolicy<Object> retryPolicy) {
        super(endPoint, false, client, retryPolicy);
    }

    @Override
    public long getMessageCount(String apiKey, String queue) {
        return getMessageCountUpTo(apiKey, queue, Long.MAX_VALUE);
    }

    @Override
    public long getMessageCountUpTo(String apiKey, String queue, long limit) {
        return doGetMessageCountUpTo(apiKey, queue, limit, false);
    }

    @Override
    public long getClaimCount(String apiKey,  String queue) {
        return doGetClaimCount(apiKey, queue);
    }

    @Override
    public List<Message> peek(String apiKey, String queue, int limit) {
        // Any server can handle this request, no need for @PartitionKey
        return doPeek(apiKey, queue, limit, false);
    }

    @Override
    public List<Message> poll(String apiKey,  String queue, Duration claimTtl, int limit) {
        return doPoll(apiKey, queue, claimTtl, limit);
    }

    @Override
    public void renew(String apiKey,/* @PartitionKey*/ String queue, Collection<String> messageIds, Duration claimTtl) {
        doRenew(apiKey, queue, messageIds, claimTtl);
    }

    @Override
    public void acknowledge(String apiKey,  String queue, Collection<String> messageIds) {
        doAcknowledge(apiKey, queue, messageIds);
    }

    @Override
    public String moveAsync(String apiKey, String from, String to) {
        // Any server can handle this request, no need for @PartitionKey
        return doMoveAsync(apiKey, from, to);
    }

    @Override
    public MoveQueueStatus getMoveStatus(String apiKey, String reference) {
        // Any server can handle this request, no need for @PartitionKey
        return doGetMoveStatus(apiKey, reference);
    }

    @Override
    public void unclaimAll(String apiKey,  String queue) {
        doUnclaimAll(apiKey, queue);
    }

    @Override
    public void purge(String apiKey,  String queue) {
        doPurge(apiKey, queue);
    }
}
