package com.bazaarvoice.emodb.common.zookeeper.store;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper around a single eventually consistent cluster-wide Map.
 */
public interface MapStore<T> {
    Map<String, T> getAll();

    Set<String> keySet();

    String getZkPath();

    @Nullable
    T get(String key);

    void set(String key, T value) throws Exception;

    void remove(String key) throws Exception;

    void addListener(MapStoreListener listener);

    void removeListener(MapStoreListener listener);
}
