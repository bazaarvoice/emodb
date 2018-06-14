package com.bazaarvoice.emodb.sor.db.astyanax;

import com.bazaarvoice.emodb.common.api.Ttls;
import com.bazaarvoice.emodb.sor.api.History;
import com.bazaarvoice.emodb.sor.core.HistoryBatchPersister;
import com.bazaarvoice.emodb.sor.core.HistoryStore;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

public class AstyanaxHistoryBatchPersister implements HistoryBatchPersister {
    private MutationBatch _mutation;
    private ColumnFamily<ByteBuffer, UUID> _columnFamily;
    private ChangeEncoder _changeEncoder;
    private HistoryStore _historyStore;
    private AstyanaxHistoryBatchPersister(MutationBatch mutation, ColumnFamily<ByteBuffer, UUID> columnFamily,
                                          ChangeEncoder changeEncoder, HistoryStore historyStore) {
        _mutation = checkNotNull(mutation);
        _columnFamily = checkNotNull(columnFamily);
        _changeEncoder = checkNotNull(changeEncoder);
        _historyStore = checkNotNull(historyStore);
    }

    @Override
    public void commit(List<History> historyList, Object rowKey) {
        if (historyList != null && !historyList.isEmpty()) {
            ColumnListMutation<UUID> historyMutation = _mutation.withRow(_columnFamily, (ByteBuffer)rowKey);
            for (History history : historyList) {
                historyMutation.putColumn(history.getChangeId(),
                        _changeEncoder.encodeHistory(history),
                        Ttls.toSeconds(_historyStore.getHistoryTtl(), 1, null));
            }
        }
    }

    public static AstyanaxHistoryBatchPersister build(MutationBatch mutation, ColumnFamily<ByteBuffer, UUID> columnFamily,
                                                      ChangeEncoder changeEncoder, HistoryStore historyStore) {
        return new AstyanaxHistoryBatchPersister(mutation, columnFamily, changeEncoder, historyStore);
    }

}
