package com.bazaarvoice.emodb.cachemgr.api;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;

import java.util.EventObject;

import static java.util.Objects.requireNonNull;

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
        _cache = requireNonNull(cache, "cache");
        _scope = requireNonNull(scope, "scope");
        _keys = requireNonNull(keys, "keys");
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
        return MoreObjects.toStringHelper(this)
                .add("cache", _cache)
                .add("scope", _scope)
                .add("keys", _keys)
                .toString();
    }
}
