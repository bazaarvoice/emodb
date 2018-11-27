package com.bazaarvoice.emodb.client.uri;

import javax.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of MultivaludedMap that has no dependencies outside native Java.
 */
public class EmoMultivaluedMap<K, V> implements MultivaluedMap<K, V>{

    private final Map<K, List<V>> _map;

    public static <K, V> EmoMultivaluedMap<K, V> create() {
        return new EmoMultivaluedMap<>();
    }

    public static <K, V> EmoMultivaluedMap<K, V> copy(MultivaluedMap<K, V> other) {
        EmoMultivaluedMap<K, V> map = new EmoMultivaluedMap<>();
        for (Map.Entry<K, List<V>> entry : other.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }

    private EmoMultivaluedMap() {
        _map = new HashMap<>();
    }

    @Override
    public void putSingle(K key, V value) {
        List<V> values = new ArrayList<>(2);
        values.add(value);
        _map.put(key, values);
    }

    @Override
    public void add(K key, V value) {
        List<V> list = _map.get(key);
        if (list != null) {
            list.add(value);
        } else {
            putSingle(key, value);
        }
    }

    @Override
    public V getFirst(K key) {
        List<V> values = _map.get(key);
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.get(0);
    }

    @Override
    public int size() {
        return _map.size();
    }

    @Override
    public boolean isEmpty() {
        return _map.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return _map.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return _map.values().stream().anyMatch(value::equals);
    }

    @Override
    public List<V> get(Object key) {
        //noinspection unchecked
        List<V> values =  _map.get((K) key);
        if (values.isEmpty()) {
            return null;
        }
        return values;
    }

    @Override
    public List<V> put(K key, List<V> value) {
        List<V> previous = _map.remove(key);
        _map.put(key, new ArrayList<>(value));
        return previous;
    }

    @Override
    public List<V> remove(Object key) {
        return _map.remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends List<V>> m) {
        for (K key : m.keySet()) {
            put(key, m.get(key));
        }
    }

    @Override
    public void clear() {
        _map.clear();
    }

    @Override
    public Set<K> keySet() {
        return _map.keySet();
    }

    @Override
    public Collection<List<V>> values() {
        List<List<V>> values = new ArrayList<>();
        for (Map.Entry<K, List<V>> entry :_map.entrySet()) {
            values.add(entry.getValue());
        }
        return values;
    }

    @Override
    public Set<Entry<K, List<V>>> entrySet() {
        Set<Entry<K, List<V>>> entrySet = new LinkedHashSet<>();
        for (Map.Entry<K, List<V>> entry :_map.entrySet()) {
            entrySet.add(entry);
        }
        return entrySet;
    }
}
