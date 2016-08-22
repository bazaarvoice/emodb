package com.bazaarvoice.emodb.common.zookeeper.store;

import com.google.common.base.Supplier;

/**
 * Wrapper around a single eventually consistent cluster-wide value.
 */
public interface ValueStore<T> extends Supplier<T> {
    T get();

    void set(T value) throws Exception;

    void addListener(ValueStoreListener listener);

    void removeListener(ValueStoreListener listener);
}
