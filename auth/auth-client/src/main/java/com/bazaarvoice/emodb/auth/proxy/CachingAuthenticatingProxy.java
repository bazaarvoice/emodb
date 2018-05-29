package com.bazaarvoice.emodb.auth.proxy;

import com.bazaarvoice.emodb.auth.InvalidCredentialException;

import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * Base class for creating an AuthenticatingProxy which caches instances by credentials.
 * @param <U> Unauthenticated interface type
 * @param <C> Credentials type
 */
abstract public class CachingAuthenticatingProxy<U, C> implements AuthenticatingProxy<U, C> {

    private final static int DEFAULT_CACHE_SIZE = 10;

    private final Map<C, ProxyEntry> _cachedInstances;
    private final int _cacheSize;

    public CachingAuthenticatingProxy() {
        this(DEFAULT_CACHE_SIZE);
    }

    public CachingAuthenticatingProxy(int cacheSize) {
        if (cacheSize <= 0) {
            throw new IllegalArgumentException("Cache size must be positive");
        }
        _cachedInstances = new HashMap<>();
        _cacheSize = cacheSize;
    }

    @Override
    public U usingCredentials(C credentials) {
        if (credentials == null) {
            throw new InvalidCredentialException("Credentials cannot be null");
        }
        // Validate the credentials here and not in createInstanceWithCredentials() to ensure validation occurs
        C validatedCredentials = validateCredentials(credentials);

        ProxyEntry proxyEntry = _cachedInstances.get(validatedCredentials);
        if (proxyEntry == null) {
            synchronized (_cachedInstances) {
                proxyEntry = _cachedInstances.get(validatedCredentials);
                if (proxyEntry == null) {
                    if (_cachedInstances.size() == _cacheSize) {
                        // Remove the least-recently used
                        _cachedInstances.entrySet().stream()
                                .min(Comparator.comparing(e -> e.getValue()._lastAccess))
                                .map(Map.Entry::getKey)
                                .ifPresent(_cachedInstances::remove);
                    }
                    proxyEntry = new ProxyEntry(createInstanceWithCredentials(validatedCredentials));
                    _cachedInstances.put(validatedCredentials, proxyEntry);
                }
            }
        }
        return proxyEntry.getProxy();
    }

    /**
     * Only validates that the credentials are well-formatted.  This does not perform any validation pertaining to
     * whether the credentials will be accepted by the destination.
     */
    abstract protected C validateCredentials(C credentials)
        throws InvalidCredentialException;

    abstract protected U createInstanceWithCredentials(C credentials);

    private class ProxyEntry {
        U _proxy;
        Instant _lastAccess;

        ProxyEntry(U proxy) {
            _proxy = proxy;
            _lastAccess = Instant.now();
        }

        U getProxy() {
            _lastAccess = Instant.now();
            return _proxy;
        }
    }
}
