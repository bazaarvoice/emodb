package com.bazaarvoice.emodb.web.scanner.control;

import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.QueueDoesNotExistException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of ScanWorkflow that uses Amazon SQS queues as the backing mechanism.  There are two queues used:
 * one for scan ranges available for uploading, and one for scan ranges that have completed uploading.
 */
public class SQSScanWorkflow implements ScanWorkflow {

    private final static int DEFAULT_TASK_CLAIM_VISIBILITY_TIMEOUT = (int) TimeUnit.MINUTES.toSeconds(10);
    private final static int DEFAULT_TASK_COMPLETE_VISIBILITY_TIMEOUT = (int) TimeUnit.MINUTES.toSeconds(5);

    private final AmazonSQS _sqs;
    private final String _pendingScanRangeQueue;
    private final String _completeScanRangeQueue;
    private final LoadingCache<String, String> _queueUrls;

    public SQSScanWorkflow(AmazonSQS sqs, String pendingScanRangeQueue, String completeScanRangeQueue) {
        _sqs = checkNotNull(sqs, "amazonSQS");
        _pendingScanRangeQueue = checkNotNull(pendingScanRangeQueue, "pendingScanRangeQueue");
        _completeScanRangeQueue = checkNotNull(completeScanRangeQueue, "completeScanRangeQueue");
        _queueUrls = CacheBuilder.newBuilder()
                .build(new CacheLoader<String, String>() {
                    @Override
                    public String load(String queueName)
                            throws Exception {
                        return queryQueueUrl(queueName);
                    }
                });
    }

    private String queryQueueUrl(String queueName) {
        try {
            return _sqs.getQueueUrl(new GetQueueUrlRequest(queueName)).getQueueUrl();
        } catch (QueueDoesNotExistException e) {
            // Create the queue
            int visibilityTimeout = queueName.equals(_pendingScanRangeQueue) ?
                    DEFAULT_TASK_CLAIM_VISIBILITY_TIMEOUT : DEFAULT_TASK_COMPLETE_VISIBILITY_TIMEOUT;
            return _sqs.createQueue(
                    new CreateQueueRequest(queueName)
                            .withAttributes(ImmutableMap.<String, String>of(
                                    "VisibilityTimeout", String.valueOf(visibilityTimeout)))
            ).getQueueUrl();
        }
    }

    private String getQueueUrl(String queueName) {
        return _queueUrls.getUnchecked(queueName);
    }

    @Override
    public void scanStatusUpdated(String scanId) {
        checkNotNull(scanId, "scanId");
        // Send a scan range complete message.  This forces the monitor to looks for available scan ranges.
        signalScanRangeComplete(scanId);
    }

    private void signalScanRangeComplete(String scanId) {
        // Place the completed range into the queue
        ScanRangeComplete complete = new QueueScanRangeComplete(scanId);
        String message = JsonHelper.asJson(complete);
        _sqs.sendMessage(new SendMessageRequest()
                .withQueueUrl(getQueueUrl(_completeScanRangeQueue))
                .withMessageBody(message));
    }

    private int toSeconds(Duration duration) {
        int seconds = (int) duration.getSeconds();
        checkArgument(seconds > 0, "TTL must be at least one second");
        return seconds;
    }

    @Override
    public ScanRangeTask addScanRangeTask(String scanId, int taskId, String placement, ScanRange range) {
        ScanRangeTask task = new QueueScanRangeTask(taskId, scanId, placement, range);
        String message = JsonHelper.asJson(task);
        _sqs.sendMessage(new SendMessageRequest()
                .withQueueUrl(getQueueUrl(_pendingScanRangeQueue))
                .withMessageBody(message));
        return task;
    }

    @Override
    public List<ScanRangeTask> claimScanRangeTasks(int max, Duration ttl) {
        if (max == 0) {
            return ImmutableList.of();
        }

        List<Message> messages = _sqs.receiveMessage(new ReceiveMessageRequest()
                .withQueueUrl(getQueueUrl(_pendingScanRangeQueue))
                .withMaxNumberOfMessages(Math.min(max, 10))           // SQS cannot claim more than 10 messages
                .withVisibilityTimeout(toSeconds(ttl))
        ).getMessages();

        return FluentIterable.from(messages)
                .transform(new Function<Message, ScanRangeTask>() {
                    @Override
                    public ScanRangeTask apply(Message message) {
                        QueueScanRangeTask task = JsonHelper.fromJson(message.getBody(), QueueScanRangeTask.class);
                        task.setMessageId(message.getReceiptHandle());
                        return task;
                    }
                })
                .toList();
    }

    @Override
    public void renewScanRangeTasks(Collection<ScanRangeTask> tasks, Duration ttl) {
        if (tasks.isEmpty()) {
            return;
        }

        int timeout = toSeconds(ttl);
        int id = 0;

        List<ChangeMessageVisibilityBatchRequestEntry> allEntries = Lists.newArrayListWithCapacity(tasks.size());
        for (ScanRangeTask task : tasks) {
            allEntries.add(
                    new ChangeMessageVisibilityBatchRequestEntry()
                            .withId(String.valueOf(id++))
                            .withReceiptHandle(((QueueScanRangeTask) task).getMessageId())
                            .withVisibilityTimeout(timeout));
        }

        // Cannot renew more than 10 in a single request
        for (List<ChangeMessageVisibilityBatchRequestEntry> entries : Lists.partition(allEntries, 10)) {
            _sqs.changeMessageVisibilityBatch(new ChangeMessageVisibilityBatchRequest()
                    .withQueueUrl(getQueueUrl(_pendingScanRangeQueue))
                    .withEntries(entries));
        }
    }

    @Override
    public void releaseScanRangeTask(ScanRangeTask task) {
        // Signal that the range is complete
        signalScanRangeComplete(task.getScanId());

        // Ack the task
        _sqs.deleteMessage(new DeleteMessageRequest()
                .withQueueUrl(getQueueUrl(_pendingScanRangeQueue))
                .withReceiptHandle(((QueueScanRangeTask) task).getMessageId()));
    }

    @Override
    public List<ScanRangeComplete> claimCompleteScanRanges(Duration ttl) {
        List<Message> messages = _sqs.receiveMessage(new ReceiveMessageRequest()
                .withQueueUrl(getQueueUrl(_completeScanRangeQueue))
                .withMaxNumberOfMessages(10)
                .withVisibilityTimeout(toSeconds(ttl))
        ).getMessages();

        return FluentIterable.from(messages)
                .transform(new Function<Message, ScanRangeComplete>() {
                    @Override
                    public ScanRangeComplete apply(Message message) {
                        QueueScanRangeComplete completion = JsonHelper.fromJson(message.getBody(), QueueScanRangeComplete.class);
                        completion.setMessageId(message.getReceiptHandle());
                        return completion;
                    }
                })
                .toList();
    }

    @Override
    public void releaseCompleteScanRanges(Collection<ScanRangeComplete> completions) {
        if (completions.isEmpty()) {
            return;
        }

        int id = 0;
        List<DeleteMessageBatchRequestEntry> entries = Lists.newArrayListWithCapacity(completions.size());
        for (ScanRangeComplete completion : completions) {
            entries.add(
                    new DeleteMessageBatchRequestEntry()
                            .withId(String.valueOf(id++))
                            .withReceiptHandle(((QueueScanRangeComplete) completion).getMessageId()));
        }

        _sqs.deleteMessageBatch(new DeleteMessageBatchRequest()
                .withQueueUrl(getQueueUrl(_completeScanRangeQueue))
                .withEntries(entries));
    }
}
