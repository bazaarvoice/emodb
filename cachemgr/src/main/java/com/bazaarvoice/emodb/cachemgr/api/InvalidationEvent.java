package com.bazaarvoice.emodb.cachemgr.api;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import java.util.EventObject;

import static com.google.common.base.Preconditions.checkNotNull;

public class InvalidationEvent extends EventObject {
    private final String _cache;
    private final InvalidationScope _scope;
    private final Optional<Iterable<String>> _keys;

    public InvalidationEvent(Object source, String cache, InvalidationScope scope) {
        this(source, cache, scope, Optional.<Iterable<String>>absent());
    }

    public InvalidationEvent(Object source, String cache, InvalidationScope scope, Iterable<String> keys) {
        this(source, cache, scope, Optional.of(keys));
    }

    private InvalidationEvent(Object source, String cache, InvalidationScope scope, Optional<Iterable<String>> keys) {
        super(source);
        _cache = checkNotNull(cache, "cache");
        _scope = checkNotNull(scope, "scope");
        _keys = checkNotNull(keys, "keys");
    }

    public String getCache() {
        return _cache;
    }

    public InvalidationScope getScope() {
        return _scope;
    }

    public boolean hasKeys() {
        return _keys.isPresent();
    }

    public Iterable<String> getKeys() {
        return _keys.orNull();
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("cache", _cache)
                .add("scope", _scope)
                .add("keys", _keys)
                .toString();
    }
}
