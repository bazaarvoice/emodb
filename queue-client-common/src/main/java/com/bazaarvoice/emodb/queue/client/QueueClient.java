package com.bazaarvoice.emodb.queue.client;

import com.bazaarvoice.emodb.client.EmoClient;
import com.bazaarvoice.emodb.queue.api.AuthQueueService;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.MoveQueueStatus;
import com.bazaarvoice.ostrich.partition.PartitionKey;

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

    /** Must match the service name in the EmoService class. */
    static final String BASE_SERVICE_NAME = "emodb-queue-1";

    /** Must match the @Path annotation on the QueueResource1 class. */
    public static final String SERVICE_PATH = "/queue/1";

    public QueueClient(URI endPoint, EmoClient client) {
        super(endPoint, false, client);
    }

    public QueueClient(URI endPoint, boolean partitionSafe, EmoClient client) {
        super(endPoint, partitionSafe, client);
    }


    @Override
    public long getMessageCount(String apiKey, String queue) {
        // Any server can handle this request, no need for @PartitionKey
        return getMessageCountUpTo(apiKey, queue, Long.MAX_VALUE);
    }

    @Override
    public long getMessageCountUpTo(String apiKey, String queue, long limit) {
        // Any server can handle this request, no need for @PartitionKey
        return doGetMessageCountUpTo(apiKey, queue, limit, false);
    }

    @Override
    public long getClaimCount(String apiKey, @PartitionKey String queue) {
        return doGetClaimCount(apiKey, queue);
    }

    @Override
    public List<Message> peek(String apiKey, String queue, int limit) {
        // Any server can handle this request, no need for @PartitionKey
        return doPeek(apiKey, queue, limit, false);
    }

    @Override
    public List<Message> poll(String apiKey, @PartitionKey String queue, Duration claimTtl, int limit) {
        return doPoll(apiKey, queue, claimTtl, limit);
    }

    @Override
    public void renew(String apiKey, @PartitionKey String queue, Collection<String> messageIds, Duration claimTtl) {
        doRenew(apiKey, queue, messageIds, claimTtl);
    }

    @Override
    public void acknowledge(String apiKey, @PartitionKey String queue, Collection<String> messageIds) {
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
    public void unclaimAll(String apiKey, @PartitionKey String queue) {
        doUnclaimAll(apiKey, queue);
    }

    @Override
    public void purge(String apiKey, @PartitionKey String queue) {
        doPurge(apiKey, queue);
    }
}
