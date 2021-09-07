package com.bazaarvoice.emodb.client.uri;

import javax.ws.rs.core.AbstractMultivaluedMap;
import javax.ws.rs.core.MultivaluedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
