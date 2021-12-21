package com.bazaarvoice.emodb.auth.shiro;

import com.google.common.collect.ImmutableList;
import org.apache.shiro.cache.Cache;
import org.apache.shiro.cache.CacheException;
import org.apache.shiro.cache.CacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Useful helper class when using invalidation caches.  There is a potential race condition due to the way Shiro
 * caches values and the way the EmoDB cache invalidation system works.  The following example describes the issue:
 *
 * <ol>
 *     <li>The realm returns authentication credentials for an API key.</li>
 *     <li>The API key is deleted, causing the authentication cache to be invalidated.</li>
 *     <li>The stale authentication credentials for the API key are still in memory and put in the cache.</li>
 * </ol>
 *
 * Since the cache never expires the incorrect API key authentication credentials are cached indefinitely.
 *
 * To combat this the cache manager can be wrapped in a ValidatingCacheManager.  This manager will
 * read the value from source the first time it is accessed.  If it differs from the cached value it will remove the
 * value from cache and return null.  If it matches the cached value then all subsequent reads simply
 * return the cached value, relying on cache invalidation if the value changes.
 *
 * With this scheme the above scenario is safe; when the stale authentication credentials are read from the cache in
 * the third step they are validated, found to be stale and removed from cache.
 */
abstract public class ValidatingCacheManager implements CacheManager {

    private final Logger _log = LoggerFactory.getLogger(getClass());

    private final CacheManager _delegate;

    public ValidatingCacheManager(CacheManager delegate) {
        _delegate = requireNonNull(delegate, "delegate");
    }

    /**
     * Subclasses should return the cache validator for the cache with the given name.  If no validator is required
     * then it should return null.
     */
    @Nullable
    abstract protected CacheValidator<?,?> getCacheValidatorForCache(String name);

    @Override
    public <K, V> Cache<K, V> getCache(String name) throws CacheException {
        //noinspection unchecked
        CacheValidator<K, V> cacheValidator = (CacheValidator<K, V>) getCacheValidatorForCache(name);

        if (cacheValidator != null) {
            return new ValidatingCache<>(name, cacheValidator);
        }

        // Return delegate cache with no validation on first get.
        return _delegate.getCache(name);
    }

    /**
     * Cache implementation that uses validating entries to validate values on the first read.
     */
    private class ValidatingCache<K, V> implements Cache<K, V> {
        private final String _name;
        private final Cache<K, ValidatingEntry> _cache;
        private final CacheValidator<K, V> _cacheValidator;

        private ValidatingCache(String name, CacheValidator<K, V> cacheValidator) {
            _name = requireNonNull(name, "name");
            _cache = _delegate.getCache(name);
            _cacheValidator = cacheValidator;
            _log.debug("Created validating cache for {} of <{}, {}>", name, cacheValidator.getKeyType(), cacheValidator.getValueType());
        }

        @Override
        public V get(K key) throws CacheException {
            ValidatingEntry entry = _cache.get(key);
            if (entry == null) {
                return null;
            }
            return entry.getValue(key);
        }

        @Override
        public V put(K key, V value) throws CacheException {
            ValidatingEntry oldEntry = _cache.put(key, new ValidatingEntry(value));
            if (oldEntry == null) {
                return null;
            }
            return oldEntry.getCachedValue();
        }

        @Override
        public V remove(K key) throws CacheException {
            ValidatingEntry oldEntry = _cache.remove(key);
            if (oldEntry == null) {
                return null;
            }
            return oldEntry.getCachedValue();
        }

        @Override
        public void clear() throws CacheException {
            _cache.clear();
        }

        @Override
        public int size() {
            return _cache.size();
        }

        @Override
        public Set<K> keys() {
            return _cache.keys();
        }

        @Override
        public Collection<V> values() {
            ImmutableList.Builder<V> values = ImmutableList.builder();

            for (K key : _cache.keys()) {
                ValidatingEntry entry = _cache.get(key);
                V value;
                if (entry != null && (value = entry.getValue(key)) != null) {
                    values.add(value);
                }
            }

            return values.build();
        }

        /**
         * Entry implementation which validates the cached value the first time it is read.  If the cache value
         * fails validation it is removed from cache.
         */
        private class ValidatingEntry {
            private final V _cachedValue;
            private volatile boolean _validated = false;

            private ValidatingEntry(V cachedValue) {
                _cachedValue = cachedValue;
            }

            public V getValue(K key) {
                if (!_validated) {
                    synchronized(this) {
                        if (!_validated) {
                            if (!_cacheValidator.getKeyType().isInstance(key) ||
                                    !_cacheValidator.getValueType().isInstance(_cachedValue) ||
                                    !_cacheValidator.isCurrentValue(key, _cachedValue)) {
                                _cache.remove(key);
                                _log.debug("Validation failed for key {} in cache {}; cached value evicted", key, _name);
                                return null;
                            }

                            _log.debug("Validation passed for key {} in cache {}", key, _name);
                            _validated = true;
                        }
                    }
                }

                return _cachedValue;
            }

            public V getCachedValue() {
                return _cachedValue;
            }
        }
    }

    /**
     * Base class for defining the validation for whether a cached value is current or stale.  Client classes are expected
     * to create their own implementations to be returned by {@link #getCacheValidatorForCache(String)}.
     */
    abstract public static class CacheValidator<CVK, CVV> {
        private final Class<CVK> _keyType;
        private final Class<CVV> _valueType;

        public CacheValidator(Class<CVK> keyType, Class<CVV> valueType) {
            _keyType = requireNonNull(keyType, "keyType");
            _valueType = requireNonNull(valueType, "valueType");
        }

        public Class<CVK> getKeyType() {
            return _keyType;
        }

        public Class<CVV> getValueType() {
            return _valueType;
        }

        abstract public boolean isCurrentValue(CVK key, CVV value);
    }
}
