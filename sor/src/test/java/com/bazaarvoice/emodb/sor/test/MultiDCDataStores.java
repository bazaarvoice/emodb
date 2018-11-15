package com.bazaarvoice.emodb.sor.test;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.audit.DiscardingAuditWriter;
import com.bazaarvoice.emodb.sor.compactioncontrol.InMemoryCompactionControlSource;
import com.bazaarvoice.emodb.sor.condition.Conditions;
import com.bazaarvoice.emodb.sor.core.DatabusEventWriterRegistry;
import com.bazaarvoice.emodb.sor.core.HistoryStore;
import com.bazaarvoice.emodb.sor.core.DefaultDataStore;
import com.bazaarvoice.emodb.sor.core.test.InMemoryHistoryStore;
import com.bazaarvoice.emodb.sor.core.test.InMemoryMapStore;
import com.bazaarvoice.emodb.sor.db.test.InMemoryDataReaderDAO;
import com.bazaarvoice.emodb.sor.log.NullSlowQueryLog;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.bazaarvoice.emodb.table.db.test.InMemoryTableDAO;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.MoreExecutors;

import java.net.URI;
import java.time.Clock;

/**
 * Wrapper around a set of {@link DataStore} instances that replicate to each other,
 * simulating a set of eventually consistent data centers.
 * <p>
 * Replication can be stopped and started to simulate replication lag in unit tests.
 */
public class MultiDCDataStores {

    private final DataStore[] _stores;
    private final InMemoryDataReaderDAO[] _inMemoryDaos;
    private final ReplicatingDataWriterDAO[] _replDaos;
    private final HistoryStore[] _historyStores;
    private final InMemoryTableDAO _tableDao;

    public MultiDCDataStores(int numDCs, MetricRegistry metricRegistry) {
        this(numDCs, false, metricRegistry);
    }

    public MultiDCDataStores(int numDCs, boolean asyncCompacter, MetricRegistry metricRegistry) {
        _tableDao = new InMemoryTableDAO();

        // create multiple data stores, one per data center
        _inMemoryDaos = new InMemoryDataReaderDAO[numDCs];
        _replDaos = new ReplicatingDataWriterDAO[numDCs];
        for (int i = 0; i < numDCs; i++) {
            _inMemoryDaos[i] = new InMemoryDataReaderDAO();
            _replDaos[i] = new ReplicatingDataWriterDAO(_inMemoryDaos[i]);
        }
        // connect each data store to every other data store
        for (ReplicatingDataWriterDAO src : _replDaos) {
            for (ReplicatingDataWriterDAO dst : _replDaos) {
                if (src != dst) {
                    src.replicateTo(dst);
                }
            }
        }
        _historyStores = new HistoryStore[numDCs];
        _stores = new DataStore[numDCs];
        for (int i = 0; i < numDCs; i++) {
            _historyStores[i] = new InMemoryHistoryStore();
            if (asyncCompacter) {
                _stores[i] = new DefaultDataStore(new SimpleLifeCycleRegistry(), metricRegistry, new DatabusEventWriterRegistry(), _tableDao,
                        _inMemoryDaos[i].setHistoryStore(_historyStores[i]), _replDaos[i], new NullSlowQueryLog(), _historyStores[i],
                        Optional.<URI>absent(),  new InMemoryCompactionControlSource(), Conditions.alwaysFalse(), new DiscardingAuditWriter(), new InMemoryMapStore<>(), Clock.systemUTC());
            } else {
                _stores[i] = new DefaultDataStore(new DatabusEventWriterRegistry(), _tableDao, _inMemoryDaos[i].setHistoryStore(_historyStores[i]),
                        _replDaos[i], new NullSlowQueryLog(), MoreExecutors.sameThreadExecutor(), _historyStores[i],
                        Optional.<URI>absent(), new InMemoryCompactionControlSource(), Conditions.alwaysFalse(),
                        new DiscardingAuditWriter(), new InMemoryMapStore<>(), metricRegistry, Clock.systemUTC());
            }
        }
    }

    public HistoryStore historyStore(int index) {
        return _historyStores[index];
    }

    public TableDAO tableDao() {
        return _tableDao;
    }

    public DataStore dc(int index) {
        return _stores[index];
    }

    public InMemoryDataReaderDAO dao(int index) {
        return _inMemoryDaos[index];
    }

    public void stopReplication() {
        for (ReplicatingDataWriterDAO dao : _replDaos) {
            dao.stopReplication();
        }
    }

    public void startReplication() {
        for (ReplicatingDataWriterDAO dao : _replDaos) {
            dao.startReplication();
        }
    }

    public void onlyReplicateDeletesUponCompactions() {
        for (ReplicatingDataWriterDAO dao : _replDaos) {
            dao.onlyReplicateDeletesUponCompaction();
        }
    }

    public void replicateCompactionDeltas() {
        for (ReplicatingDataWriterDAO dao : _replDaos) {
            dao.replicateCompactionDeltas();
        }
    }

    public MultiDCDataStores setFullConsistencyDelayMillis(int fullConsistencyDelayMillis) {
        for (InMemoryDataReaderDAO dao : _inMemoryDaos) {
            dao.setFullConsistencyDelayMillis(fullConsistencyDelayMillis);
        }
        return this;
    }

    public MultiDCDataStores setFullConsistencyTimestamp(long fullConsistencyTimestamp) {
        for (InMemoryDataReaderDAO dao : _inMemoryDaos) {
            dao.setFullConsistencyTimestamp(fullConsistencyTimestamp);
        }
        return this;
    }
}
