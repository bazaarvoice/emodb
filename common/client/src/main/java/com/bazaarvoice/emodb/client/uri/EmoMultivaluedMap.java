package com.bazaarvoice.emodb.client.uri;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.ws.rs.core.MultivaluedMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashMap;
import javax.ws.rs.core.AbstractMultivaluedMap;


/**
 * Implementation of MultivaludedMap that has no dependencies on Jersey.
 */
public class EmoMultivaluedMap<K, V> extends AbstractMultivaluedMap<K, V>{

    /**
     * Initialize the backing store in the abstract parent multivalued map
     * implementation.
     *
     * @throws NullPointerException in case the underlying {@code store} parameter
     *                              is {@code null}.
     */
    public EmoMultivaluedMap() {
        super(new HashMap<>());
    }

    public EmoMultivaluedMap(MultivaluedMap<? extends K, ? extends V> map) {
        this();
        putAll(map);
    }

    private <T extends K, U extends V> void putAll(MultivaluedMap<T, U> map) {
        for (Entry<T, List<U>> e : map.entrySet()) {
            store.put(e.getKey(), new ArrayList<V>(e.getValue()));
        }
    }
}
