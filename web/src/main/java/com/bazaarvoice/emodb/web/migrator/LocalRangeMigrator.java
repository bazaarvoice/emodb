package com.bazaarvoice.emodb.web.migrator;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.LifeCycleRegistry;
import com.bazaarvoice.emodb.sor.core.MigratorTools;
import com.bazaarvoice.emodb.web.scanner.rangescan.RangeScanUploaderResult;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class LocalRangeMigrator implements Managed {

    private final static int PIPELINE_THREAD_COUNT = 5;
    private final static int PIPELINE_BATCH_SIZE = 2500;
    // Interval for checking whether all transfers are complete and logging the result
    private final static Duration WAIT_FOR_ALL_TRANSFERS_COMPLETE_CHECK_INTERVAL = Duration.standardSeconds(30);
    // Interval after which the task will fail if there has been no progress on any active transfers
    private final static Duration WAIT_FOR_ALL_TRANSFERS_COMPLETE_TIMEOUT = Duration.standardMinutes(5);
    // Stop migrating and resplit if we receive three times the number of rows expected in a single task.
    private final static float RESPLIT_FACTOR = 3;

    private final static Logger _log = LoggerFactory.getLogger(LocalRangeMigrator.class);

    private final Counter _activeRangeMigrations;
    private final Counter _blockedRangeMigrations;
    private final Counter _activeBatches;
    private final Counter _batchRowsSubmitted;
    private final Counter _batchesSubmitted;
    private final Counter _waitingForBatchesComplete;
    private final Counter _waitingForAllTransfersComplete;
    private final Meter _failedRangeMigrations;

    private final LoadingCache<String, Counter> _rawBytesMigrated;

    private final int _threadCount;
    private final int _batchSize;
    private final MigratorTools _migratorTools;
    private final Set<ExecutorService> _batchServices = Collections.synchronizedSet(Sets.<ExecutorService>newIdentityHashSet());
    private final Duration _waitForAllTransfersCompleteCheckInterval;
    private final Duration _waitForAllTransfersCompleteTimeout;
    private ScheduledExecutorService _timeoutService;
    private volatile boolean _shutdown = true;

    @Inject
    public LocalRangeMigrator(MigratorTools migratorTools, LifeCycleRegistry lifecyle, MetricRegistry metricRegistry) {
        this(migratorTools, lifecyle, metricRegistry, PIPELINE_THREAD_COUNT, PIPELINE_BATCH_SIZE,
                WAIT_FOR_ALL_TRANSFERS_COMPLETE_CHECK_INTERVAL, WAIT_FOR_ALL_TRANSFERS_COMPLETE_TIMEOUT);
    }

    public LocalRangeMigrator(MigratorTools migratorTools, LifeCycleRegistry lifecycle, final MetricRegistry metricRegistry, int threadCount,
                              int batchSize, Duration waitForAllTransfersCompleteCheckInterval, Duration waitForAllTransfersCompleteTimeout) {
        _migratorTools = migratorTools;
        _threadCount = threadCount;
        _batchSize = batchSize;
        _waitForAllTransfersCompleteCheckInterval = waitForAllTransfersCompleteCheckInterval;
        _waitForAllTransfersCompleteTimeout = waitForAllTransfersCompleteTimeout;

        _activeRangeMigrations = metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "active-range-migrations"));
        _blockedRangeMigrations = metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "blocked-range-migrations"));
        _activeBatches = metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "active-batches"));
        _batchRowsSubmitted = metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "batch-rows-submitted"));
        _batchesSubmitted = metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "batches-submitted"));
        _waitingForBatchesComplete = metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "waiting-for-batches-complete"));
        _waitingForAllTransfersComplete = metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "waiting-for-all-transfers-complete"));
        _failedRangeMigrations = metricRegistry.meter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "failed-range-migrations"));

        // TODO: figure out if we need shards like the LocalRangeScanUploader

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
        _timeoutService = Executors.newScheduledThreadPool(1, new ThreadFactoryBuilder().setNameFormat("range-scan-timeout-%d").build());
        _shutdown = false;
    }

    @Override
    public void stop() throws Exception {
        _shutdown = true;
        _timeoutService.shutdownNow();
        synchronized(_batchServices) {
            for (ExecutorService service : _batchServices) {
                service.shutdownNow();
            }
            _batchServices.clear();
        }
    }

    public RangeScanUploaderResult migrate() {
        
    }


}
