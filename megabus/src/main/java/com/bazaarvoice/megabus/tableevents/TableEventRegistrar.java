package com.bazaarvoice.megabus.tableevents;

import com.bazaarvoice.emodb.table.db.eventregistry.TableEventRegistry;
import com.bazaarvoice.megabus.MegabusApplicationId;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class TableEventRegistrar extends AbstractScheduledService {

    private final TableEventRegistry _tableEventRegistry;
    private final MetricRegistry _metricRegistry;
    private final String _applicationId;


    @Inject
    public TableEventRegistrar(@MegabusApplicationId String applicationId,
                               TableEventRegistry tableEventRegistry,
                               MetricRegistry metricRegistry) {
        _tableEventRegistry = requireNonNull(tableEventRegistry);
        _metricRegistry = requireNonNull(metricRegistry);
        _applicationId = requireNonNull(applicationId);
    }

    @Override
    protected void startUp() throws Exception {
        register();
    }

    @Override
    protected void shutDown() throws Exception {
        super.shutDown();
    }

    @Override
    protected void runOneIteration() throws Exception {
        try {
            register();
        } catch (Exception e) {

        }
    }

    private void register() {
        _tableEventRegistry.registerTableListener(_applicationId, Instant.now().plus(7, ChronoUnit.DAYS));
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, 1, TimeUnit.HOURS);
    }
}
