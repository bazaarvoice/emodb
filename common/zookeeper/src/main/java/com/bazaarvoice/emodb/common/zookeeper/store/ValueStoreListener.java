package com.bazaarvoice.emodb.common.zookeeper.store;

public interface ValueStoreListener {
    void valueChanged() throws Exception;
}
