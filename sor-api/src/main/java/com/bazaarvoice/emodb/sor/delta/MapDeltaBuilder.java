package com.bazaarvoice.emodb.sor.delta;

import javax.annotation.Nullable;
import java.util.Map;

public interface MapDeltaBuilder extends DeltaBuilder {

    MapDeltaBuilder remove(String key);

    MapDeltaBuilder removeAll(String... key);

    MapDeltaBuilder removeAll(Iterable<String> keys);

    MapDeltaBuilder remove(String key, @Nullable Object json);

    MapDeltaBuilder removeAll(Map<String, ?> json);

    MapDeltaBuilder put(String key, @Nullable Object json);

    MapDeltaBuilder putAll(Map<String, ?> json);

    MapDeltaBuilder putIfAbsent(String key, @Nullable Object json);

    MapDeltaBuilder update(String key, Delta delta);

    MapDeltaBuilder updateAll(Map<String, Delta> deltas);

    MapDeltaBuilder updateIfExists(String key, Delta delta);

    MapDeltaBuilder retain(String key);

    MapDeltaBuilder retainAll(String... keys);

    MapDeltaBuilder retainAll(Iterable<String> keys);

    MapDeltaBuilder deleteIfEmpty();

    MapDeltaBuilder deleteIfEmpty(boolean deleteIfEmpty);

    MapDeltaBuilder removeRest();

    MapDeltaBuilder removeRest(boolean removeRest);
}
