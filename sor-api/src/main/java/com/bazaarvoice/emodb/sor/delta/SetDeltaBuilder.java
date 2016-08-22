package com.bazaarvoice.emodb.sor.delta;

import javax.annotation.Nullable;

public interface SetDeltaBuilder extends DeltaBuilder {

    SetDeltaBuilder remove(@Nullable Object value);

    SetDeltaBuilder removeAll(Object... values);

    SetDeltaBuilder removeAll(Iterable<Object> values);

    SetDeltaBuilder add(@Nullable Object value);

    SetDeltaBuilder addAll(Object... values);

    SetDeltaBuilder addAll(Iterable<Object> values);

    SetDeltaBuilder deleteIfEmpty();

    SetDeltaBuilder deleteIfEmpty(boolean deleteIfEmpty);

    SetDeltaBuilder removeRest();

    SetDeltaBuilder removeRest(boolean removeRest);
}