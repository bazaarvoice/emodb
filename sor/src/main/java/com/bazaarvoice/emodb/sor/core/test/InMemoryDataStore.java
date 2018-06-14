package com.bazaarvoice.emodb.sor.core.test;

import com.bazaarvoice.emodb.sor.compactioncontrol.InMemoryCompactionControlSource;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DefaultDataStore;
import com.bazaarvoice.emodb.sor.db.test.InMemoryDataReaderDAO;
import com.bazaarvoice.emodb.sor.log.NullSlowQueryLog;
import com.bazaarvoice.emodb.table.db.test.InMemoryTableDAO;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.MoreExecutors;

import java.net.URI;

/**
 * In-memory implementation of {@link com.bazaarvoice.emodb.sor.api.DataStore}, for testing. Doesn't generate events.
 */
public class InMemoryDataStore extends DefaultDataStore {

    public InMemoryDataStore(MetricRegistry metricRegistry) {
        this(new InMemoryDataReaderDAO(), metricRegistry);
    }

    public InMemoryDataStore(InMemoryDataReaderDAO dataDao, MetricRegistry metricRegistry) {
        this(new EventBus(), dataDao, metricRegistry);
    }

    public InMemoryDataStore(EventBus eventBus, InMemoryDataReaderDAO dataDao, MetricRegistry metricRegistry) {
        super(eventBus, new InMemoryTableDAO(), dataDao, dataDao,
                new NullSlowQueryLog(), MoreExecutors.sameThreadExecutor(), new InMemoryHistoryStore(),
                Optional.<URI>absent(), new InMemoryCompactionControlSource(), Conditions.alwaysFalse(), metricRegistry);
    }
}