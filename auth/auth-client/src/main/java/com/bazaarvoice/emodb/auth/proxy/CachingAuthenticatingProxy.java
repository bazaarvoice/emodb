package com.bazaarvoice.emodb.auth.proxy;

import com.bazaarvoice.emodb.auth.InvalidCredentialException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Base class for creating an AuthenticatingProxy which caches instances by credentials.
 * @param <U> Unauthenticated interface type
 * @param <C> Credentials type
 */
abstract public class CachingAuthenticatingProxy<U, C> implements AuthenticatingProxy<U, C> {

    private final static int DEFAULT_CACHE_SIZE = 10;

    private final LoadingCache<C, U> _cachedInstances;

    public CachingAuthenticatingProxy() {
        this(DEFAULT_CACHE_SIZE);
    }

    public CachingAuthenticatingProxy(int cacheSize) {
        checkArgument(cacheSize > 0, "Cache size must be positive");

        _cachedInstances = CacheBuilder.newBuilder()
                .maximumSize(cacheSize)
                .build(new CacheLoader<C, U>() {
                    @Override
                    public U load(C credentials) throws Exception {
                        return createInstanceWithCredentials(credentials);
                    }
                });
    }

    @Override
    public U usingCredentials(C credentials) {
        if (credentials == null) {
            throw new InvalidCredentialException("Credentials cannot be null");
        }
        // Validate the credentials here and not in createInstanceWithCredentials(), otherwise the exception
        // will be wrapped by the LoadingCache.
        return _cachedInstances.getUnchecked(validateCredentials(credentials));
    }

    /**
     * Only validates that the credentials are well-formatted.  This does not perform any validation pertaining to
     * whether the credentials will be accepted by the destination.
     */
    abstract protected C validateCredentials(C credentials)
        throws InvalidCredentialException;

    abstract protected U createInstanceWithCredentials(C credentials);
}
