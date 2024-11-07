package com.bazaarvoice.emodb.sor.core.test;

import com.bazaarvoice.emodb.sor.audit.DiscardingAuditWriter;
import com.bazaarvoice.emodb.sor.compactioncontrol.InMemoryCompactionControlSource;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DatabusEventWriterRegistry;
import com.bazaarvoice.emodb.sor.core.DefaultDataStore;
import com.bazaarvoice.emodb.sor.db.test.InMemoryDataReaderDAO;
import com.bazaarvoice.emodb.sor.kafka.KafkaProducerService;
import com.bazaarvoice.emodb.sor.log.NullSlowQueryLog;
import com.bazaarvoice.emodb.table.db.test.InMemoryTableDAO;
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.MoreExecutors;

import java.time.Clock;
import java.util.Optional;

/**
 * In-memory implementation of {@link com.bazaarvoice.emodb.sor.api.DataStore}, for testing. Doesn't generate events.
 */
public class InMemoryDataStore extends DefaultDataStore {

    public InMemoryDataStore(MetricRegistry metricRegistry, KafkaProducerService kafkaProducerService) {
        this(new InMemoryDataReaderDAO(), metricRegistry, kafkaProducerService);
    }


    public InMemoryDataStore(InMemoryDataReaderDAO dataDao, MetricRegistry metricRegistry, KafkaProducerService kafkaProducerService) {
        this(new DatabusEventWriterRegistry(), dataDao, metricRegistry, kafkaProducerService);
    }

    public InMemoryDataStore(DatabusEventWriterRegistry eventWriterRegistry, InMemoryDataReaderDAO dataDao, MetricRegistry metricRegistry, KafkaProducerService kafkaProducerService) {
        super(eventWriterRegistry, new InMemoryTableDAO(), dataDao, dataDao,
                new NullSlowQueryLog(), MoreExecutors.newDirectExecutorService(), new InMemoryHistoryStore(),
                Optional.empty(), new InMemoryCompactionControlSource(), Conditions.alwaysFalse(),
                new DiscardingAuditWriter(), new InMemoryMapStore<>(), metricRegistry, Clock.systemUTC(), kafkaProducerService);
    }
}
