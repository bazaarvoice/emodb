package com.bazaarvoice.emodb.client2.uri;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.ws.rs.core.MultivaluedMap;
import java.util.*;

/**
 * Implementation of MultivaludedMap that has no dependencies on Jersey.
 */
public class EmoMultivaluedMap<K, V> implements MultivaluedMap<K, V>{

    private final ListMultimap<K, V> _map;

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
        _map = ArrayListMultimap.create();
    }

    @Override
    public void putSingle(K key, V value) {
        _map.replaceValues(key, ImmutableList.of(value));
    }

    @Override
    public void add(K key, V value) {
        _map.put(key, value);
    }

    @Override
    public V getFirst(K key) {
        List<V> values = _map.get(key);
        if (values.isEmpty()) {
            return null;
        }
        return values.get(0);
    }

    @Override
    public void addAll(K k, V... vs) {
        _map.putAll(k, Arrays.asList(vs));
    }

    @Override
    public void addAll(K k, List<V> list) {
        _map.putAll(k, list);
    }

    @Override
    public void addFirst(K k, V v) {
    }

    @Override
    public boolean equalsIgnoreValueOrder(MultivaluedMap<K, V> multivaluedMap) {
        return false;
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
        return _map.containsValue(value);
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
        List<V> previous = _map.removeAll(key);
        _map.putAll(key, value);
        return previous.isEmpty() ? null : previous;
    }

    @Override
    public List<V> remove(Object key) {
        return _map.removeAll(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends List<V>> m) {
        for (K key : m.keySet()) {
            _map.putAll(key, m.get(key));
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
        List<List<V>> values = Lists.newArrayList();
        for (Map.Entry<K, Collection<V>> entry :_map.asMap().entrySet()) {
            values.add((List<V>) entry.getValue());
        }
        return values;
    }

    @Override
    public Set<Entry<K, List<V>>> entrySet() {
        Set<Entry<K, List<V>>> entrySet = Sets.newLinkedHashSet();
        for (Map.Entry<K, Collection<V>> entry :_map.asMap().entrySet()) {
            entrySet.add(Maps.immutableEntry(entry.getKey(), (List<V>) entry.getValue()));
        }
        return entrySet;
    }
}
