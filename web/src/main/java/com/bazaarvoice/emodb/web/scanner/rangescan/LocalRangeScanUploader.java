package com.bazaarvoice.emodb.web.scanner.rangescan;

import com.bazaarvoice.emodb.common.api.impl.LimitCounter;
import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.sor.api.CompactionControlSource;
import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.api.ReadConsistency;
import com.bazaarvoice.emodb.sor.api.StashRunTimeInfo;
import com.bazaarvoice.emodb.sor.api.StashTimeKey;
import com.bazaarvoice.emodb.sor.compactioncontrol.DelegateCompactionControl;
import com.bazaarvoice.emodb.sor.core.DataTools;
import com.bazaarvoice.emodb.sor.db.MultiTableScanOptions;
import com.bazaarvoice.emodb.sor.db.MultiTableScanResult;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.table.db.TableSet;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.writer.ScanDestinationWriter;
import com.bazaarvoice.emodb.web.scanner.writer.ScanWriter;
import com.bazaarvoice.emodb.web.scanner.writer.ScanWriterGenerator;
import com.bazaarvoice.emodb.web.scanner.writer.TransferKey;
import com.bazaarvoice.emodb.web.scanner.writer.TransferStatus;
import com.bazaarvoice.emodb.web.scanner.writer.WaitForAllTransfersCompleteResult;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLongs;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Implementation of RangeScanUploader which runs the scan synchronously in process.
 */
public class LocalRangeScanUploader implements RangeScanUploader, Managed {

    private final static int PIPELINE_THREAD_COUNT = 5;
    private final static int PIPELINE_BATCH_SIZE = 2500;
    // Interval for checking whether all transfers are complete and logging the result
    private final static Duration WAIT_FOR_ALL_TRANSFERS_COMPLETE_CHECK_INTERVAL = Duration.ofSeconds(30);
    // Interval after which the task will fail if there has been no progress on any active transfers
    private final static Duration WAIT_FOR_ALL_TRANSFERS_COMPLETE_TIMEOUT = Duration.ofMinutes(5);
    // Stop scanning and resplit if we receive three times the number of rows expected in a single task.
    private final static float RESPLIT_FACTOR = 3;

    protected final static Logger _log = LoggerFactory.getLogger(LocalRangeScanUploader.class);

    private final Counter _activeRangeScans;
    private final Counter _blockedRangeScans;
    private final Counter _activeBatches;
    private final Counter _batchRowsSubmitted;
    private final Counter _batchesSubmitted;
    private final Counter _waitingForBatchesComplete;
    private final Counter _waitingForAllTransfersComplete;
    private final Meter _failedRangeScans;
    private final Meter _hungRangeScans;

    private final LoadingCache<String, Counter> _shardsUploaded;
    private final LoadingCache<String, Counter> _rawBytesUploaded;

    private final int _threadCount;
    private final int _batchSize;
    private final DataTools _dataTools;
    private final ScanWriterGenerator _scanWriterGenerator;
    private final ObjectMapper _mapper;
    private final Set<ExecutorService> _batchServices = Collections.synchronizedSet(Sets.<ExecutorService>newIdentityHashSet());
    private final Duration _waitForAllTransfersCompleteCheckInterval;
    private final Duration _waitForAllTransfersCompleteTimeout;
    private ScheduledExecutorService _timeoutService;
    private volatile boolean _shutdown = true;

    private final CompactionControlSource _compactionControlSource;

    private final LoadingCache<String, TableSet> _tableSetByScanId;

    @Inject
    public LocalRangeScanUploader(DataTools dataTools, ScanWriterGenerator scanWriterGenerator, @DelegateCompactionControl CompactionControlSource compactionControlSource,
                                  LifeCycleRegistry lifecycle, MetricRegistry metricRegistry) {
        this(dataTools, scanWriterGenerator, compactionControlSource, lifecycle, metricRegistry, PIPELINE_THREAD_COUNT, PIPELINE_BATCH_SIZE,
                WAIT_FOR_ALL_TRANSFERS_COMPLETE_CHECK_INTERVAL, WAIT_FOR_ALL_TRANSFERS_COMPLETE_TIMEOUT);
    }

    @VisibleForTesting
    public LocalRangeScanUploader(DataTools dataTools, ScanWriterGenerator scanWriterGenerator, CompactionControlSource compactionControlSource, LifeCycleRegistry lifecycle,
                                  final MetricRegistry metricRegistry, int threadCount, int batchSize, Duration waitForAllTransfersCompleteCheckInterval,
                                  Duration waitForAllTransfersCompleteTimeout) {
        _dataTools = dataTools;
        _scanWriterGenerator = scanWriterGenerator;
        _threadCount = threadCount;
        _batchSize = batchSize;
        _waitForAllTransfersCompleteCheckInterval = waitForAllTransfersCompleteCheckInterval;
        _waitForAllTransfersCompleteTimeout = waitForAllTransfersCompleteTimeout;

        _compactionControlSource = checkNotNull(compactionControlSource, "compactionControlSource");

        // Initialize the ObjectMapper
        _mapper = new ObjectMapper();
        _mapper.disable(SerializationFeature.FLUSH_AFTER_WRITE_VALUE);
        _mapper.getFactory().setRootValueSeparator(null);  // We'll manually add a newline after every top-level object.

        _activeRangeScans = metricRegistry.counter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "active-range-scans"));
        _blockedRangeScans = metricRegistry.counter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "blocked-range-scans"));
        _activeBatches = metricRegistry.counter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "active-batches"));
        _batchRowsSubmitted = metricRegistry.counter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "batch-rows-submitted"));
        _batchesSubmitted = metricRegistry.counter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "batches-submitted"));
        _waitingForBatchesComplete = metricRegistry.counter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "waiting-for-batches-complete"));
        _waitingForAllTransfersComplete = metricRegistry.counter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "waiting-for-all-transfers-complete"));
        _failedRangeScans = metricRegistry.meter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "failed-range-scans"));
        _hungRangeScans = metricRegistry.meter(MetricRegistry.name("bv.emodb.scan", "ScanUploader", "hung-range-scans"));

        _shardsUploaded = CacheBuilder.newBuilder()
                .build(new CacheLoader<String, Counter>() {
                    @Override
                    public Counter load(String placement)
                            throws Exception {
                        return metricRegistry.counter(MetricRegistry.name("bv.emodb.scan.ScanUploader.placement", placement, "shards-uploaded"));
                    }
                });
        _rawBytesUploaded = CacheBuilder.newBuilder()
                .build(new CacheLoader<String, Counter>() {
                    @Override
                    public Counter load(String placement)
                            throws Exception {
                        return metricRegistry.counter(MetricRegistry.name("bv.emodb.scan.ScanUploader.placement", placement, "raw-bytes-uploaded"));
                    }
                });

        _tableSetByScanId = CacheBuilder.newBuilder()
                .build(new CacheLoader<String, TableSet>() {
                    @Override
                    public TableSet load(String key) throws Exception {
                        return _dataTools.createTableSet();
                    }
                });

        lifecycle.manage(this);
    }

    @Override
    public void start()
            throws Exception {
        _timeoutService = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("range-scan-timeout-%d").build());
        _shutdown = false;
    }

    @Override
    public void stop()
            throws Exception {
        _shutdown = true;
        _timeoutService.shutdownNow();
        synchronized (_batchServices) {
            for (ExecutorService service : _batchServices) {
                service.shutdownNow();
            }
            _batchServices.clear();
        }
    }

    @Override
    public RangeScanUploaderResult scanAndUpload(
            final String scanId, int taskId, ScanOptions options, final String placement, ScanRange scanRange, Date stashStartTime)
            throws IOException, InterruptedException {
        checkState(!_shutdown, "Service not started");

        final Counter shardCounter = _shardsUploaded.getUnchecked(placement);
        final Counter rawBytesUploadedCounter = _rawBytesUploaded.getUnchecked(placement);
        final ExecutorService batchService = Executors.newFixedThreadPool(_threadCount,
                new ThreadFactoryBuilder().setNameFormat("ScanRangeTask-" + taskId + "-Batch-%d").build());
        _batchServices.add(batchService);

        _log.info("Scanning placement {}: {}", placement, scanRange);
        final Instant startTime = Instant.now();

        // Enforce a time limit on how long can be spent retrieving results
        RangeScanTimeout timeout = new RangeScanTimeout(taskId);
        Future<?> timeoutFuture =
                _timeoutService.schedule(timeout, options.getMaxRangeScanTime().toMillis(), TimeUnit.MILLISECONDS);

        RangeScanHungCheck rangeScanHungCheck = null;

        final BatchContext context = new BatchContext(
                _batchSize, placement, scanRange, shardCounter, rawBytesUploadedCounter);

        _activeRangeScans.inc();
        try (ScanWriter scanWriter = _scanWriterGenerator.createScanWriter(taskId, options.getDestinations())) {
            context.setScanWriter(scanWriter);
            final ArrayBlockingQueue<Batch> queue = Queues.newArrayBlockingQueue(_threadCount);
            final Set<Thread> batchThreads = Sets.newConcurrentHashSet();

            // Create a callback to stop processing if this thread appears hung
            rangeScanHungCheck = new RangeScanHungCheck(taskId, Thread.currentThread(), batchThreads);
            _timeoutService.schedule(rangeScanHungCheck, options.getMaxRangeScanTime().plus(Duration.ofMinutes(5)).toMillis(), TimeUnit.MILLISECONDS);
            
            // Start threads for concurrently processing batch results
            for (int t = 0; t < _threadCount; t++) {
                batchService.submit(new Runnable() {
                    @Override
                    public void run() {
                        batchThreads.add(Thread.currentThread());
                        try {
                            Batch batch;
                            while (context.continueProcessing()) {
                                try {
                                    batch = queue.poll(1, TimeUnit.SECONDS);
                                    if (batch != null) {
                                        _activeBatches.inc();
                                        try {
                                            processBatch(taskId, batch);
                                        } finally {
                                            _activeBatches.dec();
                                        }
                                    }
                                } catch (Throwable t) {
                                    if (context.continueProcessing()) {
                                        _log.error("Unexpected error in scan batch processing thread", t);
                                        context.registerException(t);
                                    }
                                }
                            }

                            // Release any batches that may have been left over if we terminated early due to
                            // shutdown or some error.
                            while ((batch = queue.poll()) != null) {
                                context.closeBatch(batch, null);
                            }
                        } finally {
                            batchThreads.remove(Thread.currentThread());
                        }
                    }
                });
            }

            int partCountForFirstShard = 1;
            Batch batch = new Batch(context, partCountForFirstShard);

            // check if there is a stash cut off time.
            Map<StashTimeKey, StashRunTimeInfo> stashTimeInfoMap = _compactionControlSource.getStashTimesForPlacement(placement);
            Instant cutoffTime = stashTimeInfoMap.values().stream()
                    .map(StashRunTimeInfo::getTimestamp)
                    .min(Long::compareTo)
                    .map(Instant::ofEpochMilli)
                    .orElse(null);

            Iterator<MultiTableScanResult> allResults;

            if (options.isOnlyScanLiveRanges()) {
                allResults = _dataTools.stashMultiTableScan(scanId, placement, scanRange,
                        LimitCounter.max(), ReadConsistency.STRONG, cutoffTime);
            } else {

                MultiTableScanOptions multiTableScanOptions = new MultiTableScanOptions()
                        .setPlacement(placement)
                        .setScanRange(scanRange)
                        .setIncludeDeletedTables(false)
                        .setIncludeMirrorTables(false);
                allResults = _dataTools.multiTableScan(multiTableScanOptions, _tableSetByScanId.getUnchecked(scanId), LimitCounter.max(), ReadConsistency.STRONG, cutoffTime);
            }

            Iterator<MultiTableScanResult> nonInternalResults = Iterators.filter(allResults, result -> !result.getTable().isInternal());

            // Enforce a maximum number of results based on the scan options
            Iterator<MultiTableScanResult> results = Iterators.limit(nonInternalResults, getResplitRowCount(options));

            while (results.hasNext() && !timeout.isTimedOut()) {
                MultiTableScanResult result = results.next();
                if (batch.isFull()) {
                    // If there was an asynchronous exception since the last batch was created propagate it now
                    context.propagateExceptionIfPresent();

                    MultiTableScanResult priorResult = batch.getLastResult();
                    boolean continuedInNextBatch =
                            result.getShardId() == priorResult.getShardId() && result.getTableUuid() == priorResult.getTableUuid();

                    if (continuedInNextBatch) {
                        MultiTableScanResult firstResult = batch.getFirstResult();
                        boolean homogenousBatch =
                                result.getShardId() == firstResult.getShardId() && result.getTableUuid() == firstResult.getTableUuid();

                        if (homogenousBatch) {
                            // The entire batch contained a single shard, so increment the existing counter and reuse
                            partCountForFirstShard += 1;
                        } else {
                            // We're starting the batch with the second part for the first shard
                            partCountForFirstShard = 2;
                        }
                    } else {
                        // The batch begins with the first part
                        partCountForFirstShard = 1;
                    }

                    submitResultBatch(context, queue, batch, continuedInNextBatch);

                    batch = new Batch(context, partCountForFirstShard);
                }

                batch.add(result);
            }

            // If the loop exited with a final batch then submit it now
            submitResultBatch(context, queue, batch, false);

            // Signal that all batches for this task have been submitted
            context.signalAllBatchesSubmitted();

            // If there are any open batches wait for them to complete
            context.waitForAllBatchesComplete();

            // If there are any asynchronous transfers still taking place wait for them to complete before returning
            waitForAllTransfersComplete(taskId, scanRange, scanWriter);

            // If the results were non-empty but left additional rows then that means we received significantly more
            // rows than expected or took longer to read all rows than permitted.  Return that the remaining rows need
            // to be resplit and rescanned.
            if (allResults.hasNext() && !batch.isEmpty()) {
                _log.warn("Scanning placement {} for {}: {}", timeout.isTimedOut() ? "timed out" : "was oversized",
                        placement, scanRange);
                // Scan ranges are exclusive on the start key so resend the last key read to start on the next row.
                return RangeScanUploaderResult.resplit(
                        ScanRange.create(batch.getLastResult().getRowKey(), scanRange.getTo()));
            }

            _log.info("Scanning placement complete for task id={}, {}: {} ({})", taskId, placement, scanRange,
                    Duration.between(startTime, Instant.now()));

            return RangeScanUploaderResult.success();
        } catch (Throwable t) {
            if (Thread.interrupted()) {
                _log.error("Scanning placement failed and interrupted for task id={}, {}: {}", taskId, placement, scanRange, t);
            } else {
                _log.error("Scanning placement failed for task id={}, {}: {}", taskId, placement, scanRange, t);
            }
            context.registerException(t);
            _failedRangeScans.mark(1);
            return RangeScanUploaderResult.failure();
        } finally {
            _activeRangeScans.dec();
            if (rangeScanHungCheck != null) {
                rangeScanHungCheck.rangeScanComplete();
            }
            timeoutFuture.cancel(false);

            if (!batchService.isShutdown()) {
                batchService.shutdownNow();
            }
            _batchServices.remove(batchService);
        }
    }

    private void submitResultBatch(BatchContext context, ArrayBlockingQueue<Batch> queue, Batch batch, boolean continuedInNextBatch)
            throws IOException, InterruptedException {
        if (!batch.isEmpty()) {
            // Mark this batch as open
            context.openBatch(batch);

            try {
                batch.setContinuedInNextBatch(continuedInNextBatch);

                // Attempt to submit the batch to the result queue without blocking
                if (!queue.offer(batch)) {
                    // The queue was full.  Increment the blocked counter and synchronously wait for queue availability
                    _blockedRangeScans.inc();
                    try {
                        while (!queue.offer(batch, 5, TimeUnit.SECONDS)) {
                            context.propagateExceptionIfPresent();
                        }
                    } finally {
                        _blockedRangeScans.dec();
                    }
                }
                _batchesSubmitted.inc();
                _batchRowsSubmitted.inc(batch.getResults().size());
            } catch (IOException | InterruptedException e) {
                // Batch was never submitted so un-mark that it is open
                context.closeBatch(batch, e);
                throw e;
            }
        }
    }

    private void waitForAllTransfersComplete(int taskId, ScanRange scanRange, ScanWriter scanWriter)
            throws IOException, InterruptedException {
        _log.debug("Waiting for all transfers complete: id={}, range={}...", taskId, scanRange);
        Instant startWaitTime = Instant.now();
        Instant lastCheckTime = null;
        Instant startNoProgressTime = null;
        Map<TransferKey, TransferStatus> lastStatusMap = null;

        _waitingForAllTransfersComplete.inc();
        try {
            while (true) {
                WaitForAllTransfersCompleteResult result = scanWriter.waitForAllTransfersComplete(_waitForAllTransfersCompleteCheckInterval);
                if (result.isComplete()) {
                    return;
                }

                Instant now = Instant.now();

                // Get the transfer status for all active transfers
                Map<TransferKey, TransferStatus> currentStatusMap = result.getActiveTransferStatusMap();

                if (lastStatusMap != null) {
                    // Check if there has been any progress since the last iteration
                    boolean progressMade = false;
                    for (TransferStatus lastStatus : lastStatusMap.values()) {
                        TransferStatus currentStatus = currentStatusMap.get(lastStatus.getKey());
                        if (currentStatus == null ||
                                currentStatus.getAttempts() > lastStatus.getAttempts() ||
                                currentStatus.getBytesTransferred() > lastStatus.getBytesTransferred()) {
                            // Either the transfer completed (is now null) or has made progress since the last iteration
                            progressMade = true;
                            break;
                        }
                    }

                    if (progressMade) {
                        _log.info("Task {} has {} active transfers after {}", taskId, currentStatusMap.size(),
                                Duration.between(startWaitTime, now));
                        startNoProgressTime = null;
                    } else {
                        if (startNoProgressTime == null) {
                            startNoProgressTime = lastCheckTime;
                        }
                        Duration noProgressDuration = Duration.between(startNoProgressTime, now);
                        _log.info("Task {} has made no progress on {} active transfers in {}",
                                taskId, currentStatusMap.size(), noProgressDuration);

                        // If we've gone beyond the timeout for all transfers making no progress then raise an exception
                        if (noProgressDuration.compareTo(_waitForAllTransfersCompleteTimeout) > 0) {
                            throw new IOException("All transfers made no progress in " + noProgressDuration +
                                    " for task id=" + taskId);
                        }
                    }
                }

                lastStatusMap = currentStatusMap;
                lastCheckTime = now;
            }
        } finally {
            _waitingForAllTransfersComplete.dec();
            _log.debug("Waiting for all transfers complete: id={}, range={}... DONE", taskId, scanRange);
        }
    }

    private void processBatch(int taskId, Batch batch) {
        BatchContext context = batch.getContext();
        ScanWriter scanWriter = context.getScanWriter();
        Counter shardCounter = context.getShardCounter();

        ScanDestinationWriter writer = null;
        int prevShardId = UnsignedInteger.MAX_VALUE.intValue();
        long prevTableUuid = UnsignedLongs.MAX_VALUE;
        String placement = context.getPlacement();
        int totalPartsForShard = batch.getPartCountForFirstShard();

        try {
            for (Iterator<MultiTableScanResult> resultIter = batch.getResults().iterator();
                 resultIter.hasNext() && context.continueProcessing(); ) {

                MultiTableScanResult result = resultIter.next();

                // NOTE:Compaction should always be disabled as the resolved record may not be the most current version of the document with the introduction of cutoffTimes in scanning the emo docs.
                // and cannot be used for compaction without risking data loss.
                Map<String, Object> content = _dataTools.toContent(result, ReadConsistency.STRONG, Boolean.FALSE);

                int shardId = result.getShardId();
                long tableUuid = result.getTableUuid();

                // If we've switched shards then close this file and start a new one.
                if (prevShardId != shardId || prevTableUuid != tableUuid) {
                    prevShardId = shardId;
                    prevTableUuid = tableUuid;

                    if (writer != null) {
                        closeAndTransfer(writer, totalPartsForShard, shardCounter, true);
                        totalPartsForShard = 1;
                    }

                    writer = scanWriter.writeShardRows(result.getTable().getName(), placement, shardId, tableUuid);
                    _log.debug("Writing output file: {}", writer);
                }

                if (!Intrinsic.isDeleted(content)) {  // Ignore deleted objects
                    writer.writeDocument(content);
                }
            }

            if (writer != null) {
                closeAndTransfer(writer, totalPartsForShard, shardCounter, !batch.isContinuedInNextBatch());
            }

            context.closeBatch(batch, null);
        } catch (Throwable t) {
            _log.error("Uncaught exception processing batch for task id={}, placement {}: {}",
                    taskId, placement, context.getTaskRange(), t);
            context.closeBatch(batch, t);

            if (writer != null) {
                writer.closeAndCancel();
            }
        }
    }

    private void closeAndTransfer(ScanDestinationWriter writer,
                                  int partCount, Counter shardCounter, boolean isFinalPart)
            throws IOException {
        if (isFinalPart) {
            shardCounter.inc();
        }
        writer.closeAndTransferAsync(isFinalPart ? Optional.of(partCount) : Optional.<Integer>absent());
    }

    private JsonGenerator createGenerator(OutputStream out)
            throws IOException {
        JsonGenerator generator = _mapper.getFactory().createGenerator(out);
        // Disable closing the output stream on completion
        generator.configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);
        return generator;
    }

    private int getResplitRowCount(ScanOptions options) {
        return (int) Math.ceil(options.getRangeScanSplitSize() * RESPLIT_FACTOR);
    }

    /**
     * Maintains the context and contents for a set of scan results in a single batch.
     */
    private static class Batch {
        private final BatchContext _context;
        private final int _partCountForFirstShard;
        private final List<MultiTableScanResult> _results;
        private int _remaining;
        private boolean _continuedInNextBatch;

        private Batch(BatchContext context, int partCountForFirstShard) {
            _context = context;
            _results = Lists.newArrayListWithCapacity(_context.getBatchSize());
            _remaining = _context.getBatchSize();
            _partCountForFirstShard = partCountForFirstShard;
        }

        private BatchContext getContext() {
            return _context;
        }

        private int getPartCountForFirstShard() {
            return _partCountForFirstShard;
        }

        private boolean isContinuedInNextBatch() {
            return _continuedInNextBatch;
        }

        private void setContinuedInNextBatch(boolean continuedInNextBatch) {
            _continuedInNextBatch = continuedInNextBatch;
        }

        public void add(MultiTableScanResult result) {
            _results.add(result);
            _remaining -= 1;
        }

        public boolean isFull() {
            return _remaining == 0;
        }

        public boolean isEmpty() {
            return _results.isEmpty();
        }

        public List<MultiTableScanResult> getResults() {
            return _results;
        }

        public MultiTableScanResult getFirstResult() {
            return _results.get(0);
        }

        public MultiTableScanResult getLastResult() {
            return _results.get(_results.size() - 1);
        }
    }

    /**
     * Maintains the shared context across all batches and is used to synchronize flow between the main thread
     * and the batch processing threads.
     */
    private class BatchContext {
        // Static context available to all batches
        private final int _batchSize;
        private final String _placement;
        private final ScanRange _taskRange;
        private final Counter _shardCounter;
        private final Counter _rawBytesUploadedCounter;

        private ScanWriter _scanWriter;
        private final Set<Batch> _openBatches = Sets.newHashSet();
        private volatile boolean _allBatchesSubmitted;
        private final ReentrantLock _lock = new ReentrantLock();
        private final Condition _allBatchesCompleteOrExceptionExists = _lock.newCondition();
        private volatile Throwable _throwable = null;
        private volatile boolean _stopProcessing = false;

        private BatchContext(int batchSize, String placement, ScanRange taskRange,
                             Counter shardCounter, Counter rawBytesUploadedCounter) {
            _batchSize = batchSize;
            _placement = placement;
            _taskRange = taskRange;
            _shardCounter = shardCounter;
            _rawBytesUploadedCounter = rawBytesUploadedCounter;
        }

        private ScanWriter getScanWriter() {
            return _scanWriter;
        }

        private void setScanWriter(ScanWriter scanWriter) {
            _scanWriter = scanWriter;
        }

        private int getBatchSize() {
            return _batchSize;
        }

        private String getPlacement() {
            return _placement;
        }

        private ScanRange getTaskRange() {
            return _taskRange;
        }

        private Counter getShardCounter() {
            return _shardCounter;
        }

        private Counter getRawBytesUploadedCounter() {
            return _rawBytesUploadedCounter;
        }

        public void openBatch(Batch batch) {
            _lock.lock();
            try {
                _openBatches.add(batch);
            } finally {
                _lock.unlock();
            }
        }

        public void closeBatch(Batch batch, @Nullable Throwable t) {
            _lock.lock();
            try {
                // If there was an exception then only update it if another exception wasn't already registered
                if (t != null && _throwable == null) {
                    _throwable = t;
                }

                _openBatches.remove(batch);

                // If there are now no open batches or there was an exception in this or any other batch then
                // signal so now.
                if ((_openBatches.isEmpty() && _allBatchesSubmitted) || _throwable != null) {
                    signalAllBatchesCompleteOrExceptionExists();
                }
            } finally {
                _lock.unlock();
            }
        }

        public void registerException(Throwable t) {
            _lock.lock();
            try {
                // Only register the exception if there already wasn't one registered
                if (_throwable == null) {
                    _throwable = t;
                    signalAllBatchesCompleteOrExceptionExists();
                }
            } finally {
                _lock.unlock();
            }
        }

        public void signalAllBatchesSubmitted() {
            _lock.lock();
            try {
                _allBatchesSubmitted = true;
                // If all batches were already closed or there was already an exception then signal so now.
                if (_openBatches.isEmpty() || _throwable != null) {
                    signalAllBatchesCompleteOrExceptionExists();
                }
            } finally {
                _lock.unlock();
            }
        }

        private void signalAllBatchesCompleteOrExceptionExists() {
            // Tell any open batches or batch queuing to stop now.  This only has an affect if this method is
            // being called due to an exception.
            _stopProcessing = true;
            _allBatchesCompleteOrExceptionExists.signalAll();
        }

        public void waitForAllBatchesComplete()
                throws IOException, InterruptedException {
            _lock.lock();
            try {
                if (!_openBatches.isEmpty() && _throwable == null) {
                    _waitingForBatchesComplete.inc();
                    try {
                        while (!_openBatches.isEmpty() && _throwable == null && !_shutdown) {
                            _allBatchesCompleteOrExceptionExists.await(5, TimeUnit.SECONDS);
                        }
                    } finally {
                        _waitingForBatchesComplete.dec();
                    }
                }
            } finally {
                _lock.unlock();
            }

            propagateExceptionIfPresent();
        }

        public void propagateExceptionIfPresent()
                throws IOException {
            if (_throwable != null) {
                throw new IOException("Asynchronous exception during range scan batch", _throwable);
            }
        }

        public boolean continueProcessing() {
            return !_shutdown && !_stopProcessing;
        }

    }

    private static class RangeScanTimeout implements Runnable {
        private int _taskId;
        private volatile boolean _timedOut = false;

        private RangeScanTimeout(int taskId) {
            _taskId = taskId;
        }

        @Override
        public void run() {
            _log.warn("Scan range task timed out: id={}", _taskId);
            _timedOut = true;
        }

        public boolean isTimedOut() {
            return _timedOut;
        }
    }

    private class RangeScanHungCheck implements Runnable {
        private final int _taskId;
        private final Thread _scanThread;
        private final Collection<Thread> _batchThreads;
        private final AtomicBoolean _taskComplete;

        public RangeScanHungCheck(int taskId, Thread scanThread, Collection<Thread> batchThreads) {
            _taskId = taskId;
            _scanThread = scanThread;
            _batchThreads = batchThreads;
            _taskComplete = new AtomicBoolean(false);
        }

        @Override
        public void run() {
            // Check if the task is complete.  If not interrupt the scan thread and shut down its batch processing threads.
            if (!_taskComplete.get()) {
                _hungRangeScans.mark();
                _log.warn("Scan range appears hung: interrupting threads, stack traces to follow: id={}", _taskId);
                for (Thread thread : Iterables.concat(Collections.singleton(_scanThread), _batchThreads)) {
                    // Quick and dirty way to log the thread's stack trace -- put it in an Exception
                    Exception threadException = new Exception("Stack trace for " + thread.getName());
                    threadException.setStackTrace(thread.getStackTrace());
                    _log.warn("Stack trace for hung task: id={}, thread={}", _taskId, thread.getName(), threadException);
                    try {
                        thread.interrupt();
                    } catch (Exception e) {
                        _log.warn("Failed to interrupt thread for hung task: id={}, thread={}", _taskId, thread.getName(), e);
                    }
                }
            }
        }

        public void rangeScanComplete() {
            _taskComplete.set(true);
        }
    }
}
