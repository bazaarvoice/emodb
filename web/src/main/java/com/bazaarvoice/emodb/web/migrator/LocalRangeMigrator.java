package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.sor.core.MigratorTools;
import com.bazaarvoice.emodb.sor.db.MigrationScanResult;
import com.bazaarvoice.emodb.sor.db.ScanRange;
import com.bazaarvoice.emodb.web.scanner.ScanOptions;
import com.bazaarvoice.emodb.web.scanner.rangescan.RangeScanUploaderResult;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.inject.Named;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkState;

public class LocalRangeMigrator implements Managed {

    private final static int PIPELINE_THREAD_COUNT = 5;
    private final static int PIPELINE_BATCH_SIZE = 2500;
    // Interval for checking whether all transfers are complete and logging the result
    private final static Duration WAIT_FOR_ALL_TRANSFERS_COMPLETE_CHECK_INTERVAL = Duration.standardSeconds(30);
    // Interval after which the task will fail if there has been no progress on any active transfers
    private final static Duration WAIT_FOR_ALL_TRANSFERS_COMPLETE_TIMEOUT = Duration.standardMinutes(5);
    // Stop migrating and resplit if we receive three times the number of rows expected in a single task.
    private final static float RESPLIT_FACTOR = 3;

    private final static int DEFAULT_MAX_CONCURRENT_WRITES = 500;

    private final static Logger _log = LoggerFactory.getLogger(LocalRangeMigrator.class);

    private final Counter _activeRangeMigrations;
    private final Counter _blockedRangeMigrations;
    private final Counter _activeBatches;
    private final Counter _batchRowsSubmitted;
    private final Counter _batchesSubmitted;
    private final Counter _waitingForBatchesComplete;
    private final Meter _failedRangeMigrations;

    private final LoadingCache<String, Counter> _rawBytesMigrated;

    private final int _threadCount;
    private final int _batchSize;
    private final int _maxConcurrentWrites;
    private final MigratorTools _migratorTools;
    private final MigratorWriterFactory _writerFactory;
    private final Set<ExecutorService> _batchServices = Collections.synchronizedSet(Sets.<ExecutorService>newIdentityHashSet());
    private final Duration _waitForAllTransfersCompleteCheckInterval;
    private final Duration _waitForAllTransfersCompleteTimeout;
    private ScheduledExecutorService _timeoutService;
    private volatile boolean _shutdown = true;

    @Inject
    public LocalRangeMigrator(MigratorTools migratorTools, MigratorWriterFactory writerFactory, LifeCycleRegistry lifecyle, MetricRegistry metricRegistry, @Named ("maxConcurrentWrites") Optional<Integer> maxConcurrentWrites) {
        this(migratorTools, writerFactory, lifecyle, metricRegistry, PIPELINE_THREAD_COUNT, PIPELINE_BATCH_SIZE,
                WAIT_FOR_ALL_TRANSFERS_COMPLETE_CHECK_INTERVAL, WAIT_FOR_ALL_TRANSFERS_COMPLETE_TIMEOUT, maxConcurrentWrites.or(DEFAULT_MAX_CONCURRENT_WRITES));
    }

    public LocalRangeMigrator(MigratorTools migratorTools, MigratorWriterFactory writerFactory, LifeCycleRegistry lifecycle, final MetricRegistry metricRegistry, int threadCount,
                              int batchSize, Duration waitForAllTransfersCompleteCheckInterval, Duration waitForAllTransfersCompleteTimeout, int maxConcurrentWrites) {
        _migratorTools = migratorTools;
        _writerFactory = writerFactory;
        _threadCount = threadCount;
        _batchSize = batchSize;
        _waitForAllTransfersCompleteCheckInterval = waitForAllTransfersCompleteCheckInterval;
        _waitForAllTransfersCompleteTimeout = waitForAllTransfersCompleteTimeout;
        _maxConcurrentWrites = maxConcurrentWrites;

        _activeRangeMigrations = metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "active-range-migrations"));
        _blockedRangeMigrations = metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "blocked-range-migrations"));
        _activeBatches = metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "active-batches"));
        _batchRowsSubmitted = metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "batch-rows-submitted"));
        _batchesSubmitted = metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "batches-submitted"));
        _waitingForBatchesComplete = metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "waiting-for-batches-complete"));
        _failedRangeMigrations = metricRegistry.meter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "failed-range-migrations"));

        _rawBytesMigrated = CacheBuilder.newBuilder()
                .build(new CacheLoader<String, Counter>() {
                    @Override
                    public Counter load(String placement)
                            throws Exception {
                        return metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator.Migrator.placement", placement, "raw-bytes-migrated"));
                    }
                });

        lifecycle.manage(this);
    }

    @Override
    public void start() throws Exception {
        _timeoutService = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("range-migrator-timeout-%d").build());
        _shutdown = false;
    }

    @Override
    public void stop() throws Exception {
        _shutdown = true;
        _timeoutService.shutdownNow();
        synchronized (_batchServices) {
            for (ExecutorService service : _batchServices) {
                service.shutdownNow();
            }
            _batchServices.clear();
        }
    }

    public RangeScanUploaderResult migrate(final int taskId, ScanOptions options, final String placement, ScanRange range) throws IOException, InterruptedException {

        checkState(!_shutdown, "Service not started");

        final Counter rawBytesMigratedCounter = _rawBytesMigrated.getUnchecked(placement);
        final ExecutorService batchService = Executors.newFixedThreadPool(_threadCount);
        _batchServices.add(batchService);

        _log.info("Migrating placement {}: {}", placement, range);
        final long startTime = System.currentTimeMillis();

        // Enforce a time limit on how long can be spent retrieving results
        RangeMigrationTimeout timeout = new RangeMigrationTimeout(taskId);
        Future<?> timeoutFuture =
                _timeoutService.schedule(timeout, options.getMaxRangeScanTime().getMillis(), TimeUnit.MILLISECONDS);

        final BatchContext context = new BatchContext(_batchSize, placement, range, rawBytesMigratedCounter);

        _activeRangeMigrations.inc();
        try (MigratorWriter migratorWriter = _writerFactory.createMigratorWriter(taskId, placement)) {
            context.setMigratorWriter(migratorWriter);
            final ArrayBlockingQueue<Batch> queue = Queues.newArrayBlockingQueue(_threadCount);

            for (int i = 0; i < _threadCount; i++) {
                batchService.submit(() -> {
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
                            _log.error("Unexpected error in migration batch processing thread", t);
                            context.registerException(t);
                        }
                    }

                    // Release any batches that may have been left over if we terminated early due to
                    // shutdown or some error.
                    while ((batch = queue.poll()) != null) {
                        context.closeBatch(batch, null);
                    }
                });
            }

            Batch batch = new Batch(context);

            Iterator<MigrationScanResult> allResults = _migratorTools.readRows(placement, range);

            Iterator<MigrationScanResult> results = Iterators.limit(allResults, getResplitRowCount(options));

            while (results.hasNext() && !timeout.isTimedOut()) {
                MigrationScanResult result = results.next();
                if (batch.isFull()) {
                    // If there was an asynchronous exception since the last batch was created propagate it now
                    context.propagateExceptionIfPresent();

                    submitResultBatch(context, queue, batch);

                    batch = new Batch(context);
                }

                batch.add(result);
            }

            // If the loop exited with a final batch then submit it now
            submitResultBatch(context, queue, batch);

            // Signal that all batches for this task have been submitted
            context.signalAllBatchesSubmitted();

            // If there are any open batches wait for them to complete
            context.waitForAllBatchesComplete();


            // If the results were non-empty but left additional rows then that means we received significantly more
            // rows than expected or took longer to read all rows than permitted.  Return that the remaining rows need
            // to be resplit and rescanned.
            if (allResults.hasNext() && !batch.isEmpty()) {
                _log.warn("Migrating placement {} for {}: {}", timeout.isTimedOut() ? "timed out" : "was oversized",
                        placement, range);
                // Scan ranges are exclusive on the start key so resend the last key read to start on the next row.
                return RangeScanUploaderResult.resplit(
                        ScanRange.create(batch.getLastResult().getRowKey(), range.getTo()));
            }

            _log.info("Migrating placement complete for task id={}, {}: {} ({})", taskId, placement, range,
                    PeriodFormat.getDefault().print(Duration.millis(System.currentTimeMillis() - startTime).toPeriod()));

            return RangeScanUploaderResult.success();
        } catch (Throwable t) {
            _log.error("Migrating placement failed for task id={}, {}: {}", taskId, placement, range, t);
            context.registerException(t);
            _failedRangeMigrations.mark(1);
            return RangeScanUploaderResult.failure();
        } finally {
            _activeRangeMigrations.dec();
            timeoutFuture.cancel(false);

            if (batchService != null) {
                batchService.shutdown();
                _batchServices.remove(batchService);
            }
        }
    }

    private void submitResultBatch(BatchContext context, ArrayBlockingQueue<Batch> queue, Batch batch)
            throws IOException, InterruptedException {
        if (!batch.isEmpty()) {
            // Mark this batch as open
            context.openBatch(batch);

            try {
                // Attempt to submit the batch to the result queue without blocking
                if (!queue.offer(batch)) {
                    // The queue was full.  Increment the blocked counter and synchronously wait for queue availability
                    _blockedRangeMigrations.inc();
                    try {
                        while (!queue.offer(batch, 5, TimeUnit.SECONDS)) {
                            context.propagateExceptionIfPresent();
                        }
                    } finally {
                        _blockedRangeMigrations.dec();
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

    private int getResplitRowCount(ScanOptions options) {
        return (int) Math.ceil(options.getRangeScanSplitSize() * RESPLIT_FACTOR);
    }

    private void processBatch(int taskId, Batch batch) {
        BatchContext context = batch.getContext();
        MigratorWriter migratorWriter = context.getMigratorWriter();
        String placement = context.getPlacement();

        try {
            migratorWriter.writeToBlockTable(placement, batch.getResults(), _maxConcurrentWrites);

            context.closeBatch(batch, null);
        } catch (Throwable t) {
            _log.error("Uncaught exception processing batch for task id={}, placement {}: {}",
                    taskId, placement, context.getTaskRange(), t);
            context.closeBatch(batch, t);
        }
    }

    /**
     * Maintains the context and contents for a set of migrator results in a single batch.
     */
    private static class Batch {
        private final BatchContext _context;
        private final List<MigrationScanResult> _results;
        private int _remaining;

        private Batch(BatchContext context) {
            _context = context;
            _results = Lists.newArrayListWithCapacity(_context.getBatchSize());
            _remaining = _context.getBatchSize();
        }

        private BatchContext getContext() {
            return _context;
        }


        public void add(MigrationScanResult result) {
            _results.add(result);
            _remaining -= 1;
        }

        public boolean isFull() {
            return _remaining == 0;
        }

        public boolean isEmpty() {
            return _results.isEmpty();
        }

        public List<MigrationScanResult> getResults() {
            return _results;
        }

        public MigrationScanResult getFirstResult() {
            return _results.get(0);
        }

        public MigrationScanResult getLastResult() {
            return _results.get(_results.size() - 1);
        }
    }

    private class BatchContext {
        // Static context available to all batches
        private final int _batchSize;
        private final String _placement;
        private final ScanRange _taskRange;
        private final Counter _rawBytesUploadedCounter;

        private MigratorWriter _migratorWriter;
        private final Set<Batch> _openBatches = Sets.newHashSet();
        private volatile boolean _allBatchesSubmitted;
        private final ReentrantLock _lock = new ReentrantLock();
        private final Condition _allBatchesCompleteOrExceptionExists = _lock.newCondition();
        private volatile Throwable _throwable = null;
        private volatile boolean _stopProcessing = false;

        private BatchContext(int batchSize, String placement, ScanRange taskRange, Counter rawBytesUploadedCounter) {
            _batchSize = batchSize;
            _placement = placement;
            _taskRange = taskRange;
            _rawBytesUploadedCounter = rawBytesUploadedCounter;
        }

        private MigratorWriter getMigratorWriter() {
            return _migratorWriter;
        }

        private void setMigratorWriter(MigratorWriter migratorWriter) {
            _migratorWriter = migratorWriter;
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
                throw new IOException("Asynchronous exception during range migration batch", _throwable);
            }
        }

        public boolean continueProcessing() {
            return !_shutdown && !_stopProcessing;
        }

    }

    private static class RangeMigrationTimeout implements Runnable {
        private int _taskId;
        private volatile boolean _timedOut = false;

        private RangeMigrationTimeout(int taskId) {
            _taskId = taskId;
        }

        @Override
        public void run() {
            _log.warn("Migration range task timed out: id={}", _taskId);
            _timedOut = true;
        }

        public boolean isTimedOut() {
            return _timedOut;
        }
    }
}
