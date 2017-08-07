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
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import io.dropwizard.lifecycle.Managed;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Preconditions.checkState;

public class LocalRangeMigrator implements Managed {

    private final static float RESPLIT_FACTOR = 3;
    private final static int BATCH_SIZE = 2500;

    private final static Logger _log = LoggerFactory.getLogger(LocalRangeMigrator.class);

    private final Counter _activeRangeMigrations;
    private final Meter _failedRangeMigrations;

    private final MigratorTools _migratorTools;
    private volatile boolean _shutdown = true;

    @Inject
    public LocalRangeMigrator(MigratorTools migratorTools, LifeCycleRegistry lifecycle, MetricRegistry metricRegistry) {
        _migratorTools = migratorTools;

        _activeRangeMigrations = metricRegistry.counter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "active-range-migrations"));
        _failedRangeMigrations = metricRegistry.meter(MetricRegistry.name("bv.emodb.migrator", "Migrator", "failed-range-migrations"));

        lifecycle.manage(this);
    }

    @Override
    public void start() throws Exception {
        _shutdown = false;
    }

    @Override
    public void stop() throws Exception {
        _shutdown = true;
    }

    public RangeScanUploaderResult migrate(final int taskId, ScanOptions options, final String placement, ScanRange range, int maxConcurrentWrites) throws IOException, InterruptedException {

        checkState(!_shutdown, "Service not started");

        _log.info("Migrating placement {}: {}", placement, range);

        final long startTime = System.currentTimeMillis();

        _activeRangeMigrations.inc();
        try {

            Iterator<MigrationScanResult> allResults = _migratorTools.readRows(placement, range);

            Iterator<MigrationScanResult> results = Iterators.limit(allResults, getResplitRowCount(options));

            while (System.currentTimeMillis() - startTime < options.getMaxRangeScanTime().getMillis() && results.hasNext()) {

                Iterator<MigrationScanResult> batchIterator = Iterators.limit(results, BATCH_SIZE);
                _migratorTools.writeRows(placement, results, maxConcurrentWrites);

            }


            if (allResults.hasNext()) {
                _log.warn("Migrating placement {} for was oversized",
                        placement, range);
                // Migrator ranges are inclusive on the start key so send the next key to be read.
                return RangeScanUploaderResult.resplit(
                        ScanRange.create(allResults.next().getRowKey(), range.getTo()));
            }

            _log.info("Migrating placement complete for task id={}, {}: {} ({})", taskId, placement, range,
                    PeriodFormat.getDefault().print(Duration.millis(System.currentTimeMillis() - startTime).toPeriod()));

            return RangeScanUploaderResult.success();

        } catch (Throwable t) {
            _log.error("Migrating placement failed for task id={}, {}: {}", taskId, placement, range, t);
            _failedRangeMigrations.mark();
            return RangeScanUploaderResult.failure();
        } finally {
            _activeRangeMigrations.dec();
        }

    }

    private int getResplitRowCount(ScanOptions options) {
        return (int) Math.ceil(options.getRangeScanSplitSize() * RESPLIT_FACTOR);
    }
}
