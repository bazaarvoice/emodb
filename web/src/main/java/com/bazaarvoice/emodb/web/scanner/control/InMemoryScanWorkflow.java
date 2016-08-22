package com.bazaarvoice.emodb.web.scanner.control;

import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.joda.time.Duration;

import java.util.Collection;
import java.util.List;
import java.util.Queue;

/**
 * In memory implementation of ScanWorkflow.  Useful for unit testing.
 */
public class InMemoryScanWorkflow implements ScanWorkflow {

    private final Queue<InMemoryScanRangeTask> _pendingTasks = Queues.newConcurrentLinkedQueue();
    private final Queue<InMemoryScanRangeComplete> _completedTasks = Queues.newConcurrentLinkedQueue();

    @Override
    public void scanStatusUpdated(String scanId) {
        _completedTasks.add(new InMemoryScanRangeComplete(scanId));
    }

    @Override
    public ScanRangeTask addScanRangeTask(String scanId, int taskId, String placement, ScanRange range) {
        InMemoryScanRangeTask task = new InMemoryScanRangeTask(scanId, taskId, placement, range);
        _pendingTasks.add(task);
        return task;
    }

    @Override
    public List<ScanRangeTask> claimScanRangeTasks(int max, Duration ttl) {
        List<ScanRangeTask> tasks = Lists.newArrayListWithExpectedSize(Math.min(_pendingTasks.size(), max));

        InMemoryScanRangeTask task;
        while ((task = _pendingTasks.poll()) != null) {
            tasks.add(task);
        }

        return tasks;
    }

    @Override
    public void renewScanRangeTasks(Collection<ScanRangeTask> tasks, Duration ttl) {
        // no-op
    }

    @Override
    public void releaseScanRangeTask(ScanRangeTask task) {
        _completedTasks.add(new InMemoryScanRangeComplete(task.getScanId()));
    }

    @Override
    public List<ScanRangeComplete> claimCompleteScanRanges(Duration ttl) {
        List<ScanRangeComplete> completions = Lists.newArrayListWithExpectedSize(_completedTasks.size());

        InMemoryScanRangeComplete completion;
        while ((completion = _completedTasks.poll()) != null) {
            completions.add(completion);
        }

        return completions;
    }

    @Override
    public void releaseCompleteScanRanges(Collection<ScanRangeComplete> completions) {
        // no-op
    }

    public List<ScanRangeTask> peekAllPendingTasks() {
        return ImmutableList.copyOf(_pendingTasks.toArray(new ScanRangeTask[_pendingTasks.size()]));
    }

    public List<ScanRangeComplete> peekAllCompletedTasks() {
        return ImmutableList.copyOf(_completedTasks.toArray(new ScanRangeComplete[_completedTasks.size()]));
    }

    public static class InMemoryScanRangeTask implements ScanRangeTask {
        private final int _id;
        private final String _scanId;
        private final String _placement;
        private final ScanRange _range;

        public InMemoryScanRangeTask(String scanId, int id, String placement, ScanRange range) {
            _scanId = scanId;
            _id = id;
            _placement = placement;
            _range = range;
        }

        @Override
        public String getScanId() {
            return _scanId;
        }

        @Override
        public int getId() {
            return _id;
        }

        @Override
        public String getPlacement() {
            return _placement;
        }

        @Override
        public ScanRange getRange() {
            return _range;
        }
    }

    public static class InMemoryScanRangeComplete implements ScanRangeComplete {
        private final String _scanId;

        public InMemoryScanRangeComplete(String scanId) {
            _scanId = scanId;
        }

        @Override
        public String getScanId() {
            return _scanId;
        }
    }
}
