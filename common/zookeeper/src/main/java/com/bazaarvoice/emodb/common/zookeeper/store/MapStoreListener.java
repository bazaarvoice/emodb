package com.bazaarvoice.emodb.common.zookeeper.store;

public interface MapStoreListener {
    void entryChanged(String key, ChangeType changeType) throws Exception;
}
