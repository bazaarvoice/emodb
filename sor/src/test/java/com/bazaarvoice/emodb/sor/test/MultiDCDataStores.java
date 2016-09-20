package com.bazaarvoice.emodb.sor.test;

import com.bazaarvoice.emodb.common.dropwizard.lifecycle.SimpleLifeCycleRegistry;
import com.bazaarvoice.emodb.sor.api.DataStore;
import com.bazaarvoice.emodb.sor.core.AuditStore;
import com.bazaarvoice.emodb.sor.core.DefaultDataStore;
import com.bazaarvoice.emodb.sor.core.test.InMemoryAuditStore;
import com.bazaarvoice.emodb.sor.db.test.InMemoryDataDAO;
import com.bazaarvoice.emodb.sor.log.NullSlowQueryLog;
import com.bazaarvoice.emodb.table.db.TableDAO;
import com.bazaarvoice.emodb.table.db.test.InMemoryTableDAO;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.MoreExecutors;

import java.net.URI;

/**
 * Wrapper around a set of {@link DataStore} instances that replicate to each other,
 * simulating a set of eventually consistent data centers.
 * <p>
 * Replication can be stopped and started to simulate replication lag in unit tests.
 */
public class MultiDCDataStores {

    private final DataStore[] _stores;
    private final InMemoryDataDAO[] _inMemoryDaos;
    private final ReplicatingDataWriterDAO[] _replDaos;
    private final AuditStore[] _auditStores;
    private final InMemoryTableDAO _tableDao;

    public MultiDCDataStores(int numDCs, MetricRegistry metricRegistry) {
        this(numDCs, false, metricRegistry);
    }

    public MultiDCDataStores(int numDCs, boolean asyncCompacter, MetricRegistry metricRegistry) {
        _tableDao = new InMemoryTableDAO();

        // create multiple data stores, one per data center
        _inMemoryDaos = new InMemoryDataDAO[numDCs];
        _replDaos = new ReplicatingDataWriterDAO[numDCs];
        for (int i = 0; i < numDCs; i++) {
            _inMemoryDaos[i] = new InMemoryDataDAO();
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
        _auditStores = new AuditStore[numDCs];
        _stores = new DataStore[numDCs];
        for (int i = 0; i < numDCs; i++) {
            _auditStores[i] = new InMemoryAuditStore();
            if (asyncCompacter) {
                _stores[i] = new DefaultDataStore(new SimpleLifeCycleRegistry(), metricRegistry, new EventBus(), _tableDao,
                        _inMemoryDaos[i].setAuditStore(_auditStores[i]), _replDaos[i], new NullSlowQueryLog(), _auditStores[i],
                        Optional.<URI>absent());
            } else {
                _stores[i] = new DefaultDataStore(new EventBus(), _tableDao, _inMemoryDaos[i].setAuditStore(_auditStores[i]),
                        _replDaos[i], new NullSlowQueryLog(), MoreExecutors.sameThreadExecutor(), _auditStores[i],
                        Optional.<URI>absent(), metricRegistry);
            }
        }
    }

    public AuditStore auditStore(int index) {
        return _auditStores[index];
    }

    public TableDAO tableDao() {
        return _tableDao;
    }

    public DataStore dc(int index) {
        return _stores[index];
    }

    public InMemoryDataDAO dao(int index) {
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
        for (InMemoryDataDAO dao : _inMemoryDaos) {
            dao.setFullConsistencyDelayMillis(fullConsistencyDelayMillis);
        }
        return this;
    }

    public MultiDCDataStores setFullConsistencyTimestamp(long fullConsistencyTimestamp) {
        for (InMemoryDataDAO dao : _inMemoryDaos) {
            dao.setFullConsistencyTimestamp(fullConsistencyTimestamp);
        }
        return this;
    }
}
