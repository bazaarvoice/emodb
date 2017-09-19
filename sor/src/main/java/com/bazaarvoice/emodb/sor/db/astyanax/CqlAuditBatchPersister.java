package com.bazaarvoice.emodb.sor.db.astyanax;


import com.bazaarvoice.emodb.common.api.Ttls;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.core.AuditBatchPersister;
import com.bazaarvoice.emodb.sor.core.AuditStore;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import java.nio.ByteBuffer;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;

public class CqlAuditBatchPersister implements AuditBatchPersister{

    private BatchStatement _batchStatement;
    private TableDDL _tableDDL;
    private ConsistencyLevel _consistencyLevel;

    private ChangeEncoder _changeEncoder;
    private AuditStore _auditStore;

    private CqlAuditBatchPersister(BatchStatement batchStatement, TableDDL tableDDL,
                                   ChangeEncoder changeEncoder, AuditStore auditStore,
                                   ConsistencyLevel consistencyLevel) {
        _batchStatement = checkNotNull(batchStatement);
        _tableDDL = checkNotNull(tableDDL);
        _changeEncoder = checkNotNull(changeEncoder);
        _auditStore = checkNotNull(auditStore);
        _consistencyLevel = consistencyLevel;
    }


    @Override
    public void commit(List<History> historyList, Object rowKey) {
        if (historyList != null && !historyList.isEmpty()) {
            for (History history : historyList) {
                _batchStatement.add(QueryBuilder.insertInto(_tableDDL.getTableMetadata())
                        .value(_tableDDL.getRowKeyColumnName(), rowKey)
                        .value(_tableDDL.getChangeIdColumnName(), history.getChangeId())
                        .value(_tableDDL.getValueColumnName(), _changeEncoder.encodeHistory(history))
                        .using(ttl(Ttls.toSeconds(_auditStore.getHistoryTtl(), 1, null)))
                        .setConsistencyLevel(_consistencyLevel));
            }
        }
    }

    public static CqlAuditBatchPersister build(BatchStatement batchStatement, TableDDL tableDDL,
                                               ChangeEncoder changeEncoder, AuditStore auditStore,
                                               ConsistencyLevel consistencyLevel) {
        return new CqlAuditBatchPersister(batchStatement, tableDDL, changeEncoder, auditStore, consistencyLevel);
    }
}