package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.api.Ttls;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.core.AuditBatchPersister;
import com.bazaarvoice.emodb.sor.core.AuditStore;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public class AstyanaxAuditBatchPersister implements AuditBatchPersister {
    private MutationBatch _mutation;
    private ColumnFamily<ByteBuffer, UUID> _columnFamily;
    private ChangeEncoder _changeEncoder;
    private AuditStore _auditStore;
    private AstyanaxAuditBatchPersister(MutationBatch mutation, ColumnFamily<ByteBuffer, UUID> columnFamily,
                                        ChangeEncoder changeEncoder, AuditStore auditStore) {
        _mutation = checkNotNull(mutation);
        _columnFamily = checkNotNull(columnFamily);
        _changeEncoder = checkNotNull(changeEncoder);
        _auditStore = checkNotNull(auditStore);
    }

    @Override
    public void commit(List<History> historyList, Object rowKey) {
        if (historyList != null && !historyList.isEmpty()) {
            ColumnListMutation<UUID> historyMutation = _mutation.withRow(_columnFamily, (ByteBuffer)rowKey);
            for (History history : historyList) {
                historyMutation.putColumn(history.getChangeId(),
                        _changeEncoder.encodeHistory(history),
                        Ttls.toSeconds(_auditStore.getHistoryTtl(), 1, null));
            }
        }
    }

    public static AstyanaxAuditBatchPersister build(MutationBatch mutation, ColumnFamily<ByteBuffer, UUID> columnFamily,
                                               ChangeEncoder changeEncoder, AuditStore auditStore) {
        return new AstyanaxAuditBatchPersister(mutation, columnFamily, changeEncoder, auditStore);
    }

}
