package com.bazaarvoice.emodb.databus.core;

import com.bazaarvoice.emodb.sor.api.Intrinsic;
import com.bazaarvoice.emodb.sor.delta.eval.Intrinsics;
import com.bazaarvoice.emodb.table.db.Table;

class TableFilterIntrinsics implements Intrinsics {
    private final Table _table;

    TableFilterIntrinsics(Table table) {
        _table = table;
    }

    @Override
    public String getId() {
        throw new UnsupportedOperationException(Intrinsic.ID);
    }

    @Override
    public String getTable() {
        return _table.getName();
    }

    @Override
    public String getSignature() {
        throw new UnsupportedOperationException(Intrinsic.SIGNATURE);
    }

    @Override
    public boolean isDeleted() {
        throw new UnsupportedOperationException(Intrinsic.DELETED);
    }

    @Override
    public String getFirstUpdateAt() {
        throw new UnsupportedOperationException(Intrinsic.FIRST_UPDATE_AT);
    }

    @Override
    public String getLastUpdateAt() {
        throw new UnsupportedOperationException(Intrinsic.LAST_UPDATE_AT);
    }

    @Override
    public String getTablePlacement() {
        return _table.getOptions().getPlacement();
    }
}
