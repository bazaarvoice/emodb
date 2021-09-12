package com.bazaarvoice.emodb.sor.core.test;

import com.bazaarvoice.emodb.common.zookeeper.store.ChangeType;
import com.bazaarvoice.emodb.common.zookeeper.store.MapStore;
import com.bazaarvoice.emodb.common.zookeeper.store.MapStoreListener;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryMapStore<T> implements MapStore<T> {

    private final Logger _log = LoggerFactory.getLogger(InMemoryMapStore.class);

    private final Map<String, T> _store;
    private final List<MapStoreListener> _listeners;


    public InMemoryMapStore() {
        _store = new HashMap<>();
        _listeners = new ArrayList<>();
    }

    @Override
    public Map<String, T> getAll() {
        return _store;
    }

    @Override
    public Set<String> keySet() {
        return _store.keySet();
    }

    @Nullable
    @Override
    public T get(String key) {
        return _store.get(key);
    }

    @Override
    public void set(String key, T value) throws Exception {
        boolean updating = _store.containsKey(key);
        _store.put(key, value);
        fireChanged(key, updating ? ChangeType.CHANGE : ChangeType.ADD);
    }

    @Override
    public void remove(String key) throws Exception {
        if (_store.remove(key) != null) {
            fireChanged(key, ChangeType.REMOVE);
        }
    }

    @Override
    public void addListener(MapStoreListener listener) {
        _listeners.add(listener);
    }

    @Override
    public void removeListener(MapStoreListener listener) {
        _listeners.remove(listener);
    }

    private void fireChanged(String key, ChangeType changeType) {
        for (MapStoreListener listener : _listeners) {
            try {
                listener.entryChanged(key, changeType);
            } catch (Throwable t) {
                _log.error("Listener threw an unexpected exception.", t);
            }
        }

    }
}
