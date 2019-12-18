package com.bazaarvoice.megabus.tableevents;

import com.bazaarvoice.emodb.table.db.eventregistry.TableEventTools;
import com.bazaarvoice.emodb.table.db.eventregistry.TableEvent;
import com.bazaarvoice.emodb.table.db.eventregistry.TableEventRegistry;
import com.bazaarvoice.megabus.MegabusApplicationId;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TableEventProcessor extends AbstractScheduledService {

    private static final Logger _log = LoggerFactory.getLogger(TableEventProcessor.class);


    private final TableEventRegistry _tableEventRegistry;
    private final MetricRegistry _metricRegistry;
    private final String _applicationId;
    private final TableEventTools _tableEventTools;

    @Inject
    public TableEventProcessor(@MegabusApplicationId String applicationId,
                               TableEventRegistry tableEventRegistry,
                               MetricRegistry metricRegistry,
                               TableEventTools tableEventTools) {
        _tableEventRegistry = requireNonNull(tableEventRegistry);
        _metricRegistry = requireNonNull(metricRegistry);
        _applicationId = requireNonNull(applicationId);
        _tableEventTools = requireNonNull(tableEventTools);
    }

    @Override
    protected void runOneIteration() throws Exception {
        Map.Entry<String, TableEvent> tableEventPair = _tableEventRegistry.getNextTableEvent(_applicationId);

        if (tableEventPair == null) {
            return;
        }

        String table = tableEventPair.getKey();
        TableEvent tableEvent = tableEventPair.getValue();

        switch (tableEvent.getAction()) {
            case DROP:
                _log.info(_tableEventTools.getIdsForStorage(table, tableEvent.getUuid())
                        .collect(Collectors.toList())
                        .toString()
                );
                break;
            case PROMOTE:
                break;
        }
    }

    @Override
    protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, 5, TimeUnit.SECONDS);
    }
}
