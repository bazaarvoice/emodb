package com.bazaarvoice.emodb.web.scanner.control;

import com.bazaarvoice.emodb.common.json.JsonHelper;
import com.bazaarvoice.emodb.queue.api.Message;
import com.bazaarvoice.emodb.queue.api.QueueService;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of ScanWorkflow that uses EmoDB queues as the backing mechanism.  There are two queues used:
 * one for scan ranges available for uploading, and one for scan ranges that have completed uploading.
 */
public class QueueScanWorkflow implements ScanWorkflow {

    private final QueueService _queueService;
    private final String _pendingScanRangeQueue;
    private final String _completeScanRangeQueue;

    public QueueScanWorkflow(QueueService queueService, String pendingScanRangeQueue, String completeScanRangeQueue) {
        _queueService = requireNonNull(queueService, "queueService");
        _pendingScanRangeQueue = requireNonNull(pendingScanRangeQueue, "pendingScanRangeQueue");
        _completeScanRangeQueue = requireNonNull(completeScanRangeQueue, "completeScanRangeQueue");
    }

    @Override
    public void scanStatusUpdated(String scanId) {
        requireNonNull(scanId, "scanId");
        // Send a scan range complete message.  This forces the monitor to looks for available scan ranges.
        signalScanRangeComplete(scanId);
    }

    private void signalScanRangeComplete(String scanId) {
        // Place the completed range into the queue
        ScanRangeComplete complete = new QueueScanRangeComplete(scanId);
        //noinspection unchecked
        Map<String, Object> message = JsonHelper.convert(complete, Map.class);
        _queueService.send(_completeScanRangeQueue, message);

    }
    @Override
    public ScanRangeTask addScanRangeTask(String scanId, int id, String placement, ScanRange range) {
        ScanRangeTask task = new QueueScanRangeTask(id, scanId, placement, range);
        //noinspection unchecked
        Map<String, Object> message = JsonHelper.convert(task, Map.class);
        _queueService.send(_pendingScanRangeQueue, message);
        return task;
    }

    @Override
    public List<ScanRangeTask> claimScanRangeTasks(int max, Duration ttl) {
        List<Message> messages = _queueService.poll(_pendingScanRangeQueue, ttl, max);
        return FluentIterable.from(messages)
                .transform(new Function<Message, ScanRangeTask>() {
                    @Override
                    public ScanRangeTask apply(Message message) {
                        QueueScanRangeTask task = JsonHelper.convert(message.getPayload(), QueueScanRangeTask.class);
                        task.setMessageId(message.getId());
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

        Collection<String> messageIds = Collections2.transform(tasks, new Function<ScanRangeTask, String>() {
            @Override
            public String apply(ScanRangeTask task) {
                return ((QueueScanRangeTask) task).getMessageId();
            }
        });

        _queueService.renew(_pendingScanRangeQueue, messageIds, ttl);
    }

    @Override
    public void releaseScanRangeTask(ScanRangeTask task) {
        // Signal that the range is complete
        signalScanRangeComplete(task.getScanId());

        // Ack the task
        QueueScanRangeTask queueScanRangeTask = (QueueScanRangeTask) task;
        _queueService.acknowledge(_pendingScanRangeQueue, ImmutableList.of(queueScanRangeTask.getMessageId()));
    }

    @Override
    public List<ScanRangeComplete> claimCompleteScanRanges(Duration ttl) {
        List<Message> messages = _queueService.poll(_completeScanRangeQueue, ttl, 100);
        return FluentIterable.from(messages)
                .transform(new Function<Message, ScanRangeComplete>() {
                    @Override
                    public ScanRangeComplete apply(Message message) {
                        QueueScanRangeComplete completion = JsonHelper.convert(message.getPayload(), QueueScanRangeComplete.class);
                        completion.setMessageId(message.getId());
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

        Collection<String> messageIds = Collections2.transform(completions, new Function<ScanRangeComplete, String>() {
            @Override
            public String apply(ScanRangeComplete completion) {
                return ((QueueScanRangeComplete) completion).getMessageId();
            }
        });

        _queueService.acknowledge(_completeScanRangeQueue, messageIds);
    }
}
