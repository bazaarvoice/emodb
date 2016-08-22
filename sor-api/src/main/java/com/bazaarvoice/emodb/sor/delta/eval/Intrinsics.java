package com.bazaarvoice.emodb.sor.delta.eval;

public interface Intrinsics {

    String getId();

    String getTable();

    String getSignature();

    boolean isDeleted();

    String getFirstUpdateAt();

    String getLastUpdateAt();

    String getTablePlacement();
}
