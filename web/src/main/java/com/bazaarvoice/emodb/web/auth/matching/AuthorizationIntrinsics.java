package com.bazaarvoice.emodb.web.auth.matching;

import com.bazaarvoice.emodb.sor.delta.eval.Intrinsics;

/**
 * Intrinsics implementation for evaluating conditions when authorizing access to a table.
 * @see TableConditionPart
 */
public class AuthorizationIntrinsics implements Intrinsics {

    private final String _table;
    private final String _placement;

    public AuthorizationIntrinsics(String table, String placement) {
        _table = table;
        _placement = placement;
    }

    @Override
    public String getTable() {
        return _table;
    }

    @Override
    public String getTablePlacement() {
        return _placement;
    }

    @Override
    public String getId() {
        throw new UnsupportedOperationException("Cannot evaluate id in table authorization context");
    }

    @Override
    public String getSignature() {
        throw new UnsupportedOperationException("Cannot evaluate signature in table authorization context");
    }

    @Override
    public boolean isDeleted() {
        throw new UnsupportedOperationException("Cannot evaluate deleted in table authorization context");
    }

    @Override
    public String getFirstUpdateAt() {
        throw new UnsupportedOperationException("Cannot evaluate first update time in table authorization context");
    }

    @Override
    public String getLastUpdateAt() {
        throw new UnsupportedOperationException("Cannot evaluate last update time in table authorization context");
    }

    @Override
    public String getLastMutateAt() {
        throw new UnsupportedOperationException("Cannot evaluate last mutate time in table authorization context");
    }
}
