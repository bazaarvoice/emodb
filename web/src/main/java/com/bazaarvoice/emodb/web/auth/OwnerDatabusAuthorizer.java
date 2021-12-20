package com.bazaarvoice.emodb.web.auth;

import com.bazaarvoice.emodb.auth.InternalAuthorizer;
import com.bazaarvoice.emodb.common.dropwizard.time.ClockTicker;
import com.bazaarvoice.emodb.databus.auth.ConstantDatabusAuthorizer;
import com.bazaarvoice.emodb.databus.auth.DatabusAuthorizer;
import com.bazaarvoice.emodb.databus.model.OwnedSubscription;
import com.bazaarvoice.emodb.web.auth.resource.NamedResource;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.PermissionResolver;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link DatabusAuthorizer} which checks the owner's permissions for all operations.
 */
public class OwnerDatabusAuthorizer implements DatabusAuthorizer {

    private final static int DEFAULT_PERMISSION_CHECK_CACHE_SIZE = 1000;
    private final static Duration DEFAULT_PERMISSION_CHECK_CACHE_TIMEOUT = Duration.ofSeconds(2);
    private final static Duration MAX_PERMISSION_CHECK_CACHE_TIMEOUT = Duration.ofSeconds(5);
    private final static int DEFAULT_READ_PERMISSION_CACHE_SIZE = 200;

    private final InternalAuthorizer _internalAuthorizer;

    /**
     * The most expensive operation performed by this implementation happens during {@link OwnerDatabusAuthorizerForOwner#canReceiveEventsFromTable(String)}
     * when it checks whether an owner has read permission on a table.  The {@link InternalAuthorizer} implementation is
     * typically efficient and caches as much information as possible to make the evaluation quickly.  However, this method
     * is called as part of the databus fanout process and thus happens at high scale, so every bit of efficiency
     * achieved for this computation is beneficial.
     *
     * There is typically high temporal locality for this method call, since an owner may have multiple subscriptions and
     * updates to a single table are commonly clustered.  For this reason caching the permissions reduce the average
     * time for the method.  However, if the permissions for an owner are modified then any cached results may no longer
     * be valid.  For this reason the results are cached for only a brief period.  This means that when a user's permissions
     * change it may take several seconds for affected tables' data to be included in or excluded from that user's
     * subscriptions.  However, given the eventually consistent nature of EmoDB and the efficiency gained by caching
     * this is a tolerable trade-off.
     */
     private final LoadingCache<OwnerTableCacheKey, Boolean> _permissionCheckCache;

    /**
     * As described previously calls to {@link OwnerDatabusAuthorizerForOwner#canReceiveEventsFromTable(String)} are
     * typically temporally clustered by table.  Instantiation the read permission for a table is relatively inexpensive
     * but again for the sake of efficiency during databus fanout should be minimized.  Unlike with the previous cache,
     * however, the permission instance is not tied to any particular user and can be cached indefinitely with no
     * negative efffects.  For this reason the permissions are cached separately to reduce frequent permission resolution.
     */
    private final LoadingCache<String, Permission> _readPermissionCache;

    private final PermissionResolver _permissionResolver;

    @Inject
    public OwnerDatabusAuthorizer(InternalAuthorizer internalAuthorizer, final PermissionResolver permissionResolver,
                                  MetricRegistry metricRegistry, Clock clock) {
        this(internalAuthorizer, permissionResolver, metricRegistry, clock, DEFAULT_PERMISSION_CHECK_CACHE_SIZE,
                DEFAULT_PERMISSION_CHECK_CACHE_TIMEOUT, DEFAULT_READ_PERMISSION_CACHE_SIZE);
    }

    public OwnerDatabusAuthorizer(InternalAuthorizer internalAuthorizer, final PermissionResolver permissionResolver,
                                  MetricRegistry metricRegistry, Clock clock, int permissionCheckCacheSize,
                                  Duration permissionCheckCacheTimeout, int readPermissionCacheSize) {
        _internalAuthorizer = requireNonNull(internalAuthorizer, "internalAuthorizer");
        _permissionResolver = requireNonNull(permissionResolver, "permissionResolver");

        if (permissionCheckCacheSize > 0) {
            requireNonNull(permissionCheckCacheTimeout, "permissionCheckCacheTimeout");
            checkArgument(permissionCheckCacheTimeout.compareTo(MAX_PERMISSION_CHECK_CACHE_TIMEOUT) <= 0,
                    "Permission check cache timeout is too long");

            _permissionCheckCache = CacheBuilder.newBuilder()
                    .maximumSize(permissionCheckCacheSize)
                    .expireAfterWrite(permissionCheckCacheTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .recordStats()
                    .ticker(ClockTicker.getTicker(clock))
                    .build(new CacheLoader<OwnerTableCacheKey, Boolean>() {
                        @Override
                        public Boolean load(OwnerTableCacheKey key) throws Exception {
                            return ownerCanReadTable(key._ownerId, key._table);
                        }
                    });

            if (metricRegistry != null) {
                // Getting the full benefits of permission check caching requires tuning.  Publish statistics to
                // give visibility into performance.
                metricRegistry.register(MetricRegistry.name("bv.emodb.databus", "authorizer", "read-permission-cache", "hits"),
                        new Gauge<Long>() {
                            @Override
                            public Long getValue() {
                                return _permissionCheckCache.stats().hitCount();
                            }
                        });

                metricRegistry.register(MetricRegistry.name("bv.emodb.databus", "authorizer", "read-permission-cache", "misses"),
                        new Gauge<Long>() {
                            @Override
                            public Long getValue() {
                                return _permissionCheckCache.stats().missCount();
                            }
                        });
            }
        } else {
            _permissionCheckCache = null;
        }

        if (readPermissionCacheSize > 0) {
            _readPermissionCache = CacheBuilder.newBuilder()
                    .maximumSize(readPermissionCacheSize)
                    .ticker(ClockTicker.getTicker(clock))
                    .build(new CacheLoader<String, Permission>() {
                        @Override
                        public Permission load(String table) throws Exception {
                            return createReadPermission(table);
                        }
                    });
        } else {
            _readPermissionCache = null;
        }
    }

    @Override
    public DatabusAuthorizerByOwner owner(String ownerId) {
        // TODO:  To grandfather in subscriptions before API keys were enforced the following code
        //        permits all operations if there is no owner.  This code should be replaced with the
        //        commented-out version once enough time has passed for all grandfathered-in
        //        subscriptions to have been renewed and therefore have an owner attached.
        //
        // return new OwnerDatabusAuthorizerForOwner(requireNonNull(ownerId, "ownerId"));

        if (ownerId != null) {
            return new OwnerDatabusAuthorizerForOwner(ownerId);
        } else {
            return ConstantDatabusAuthorizer.ALLOW_ALL.owner(ownerId);
        }
    }

    private class OwnerDatabusAuthorizerForOwner implements DatabusAuthorizerByOwner {
        private final String _ownerId;

        private OwnerDatabusAuthorizerForOwner(String ownerId) {
            _ownerId = ownerId;
        }

        /**
         * A subscription can be accessed if either of the following conditions are met:
         * <ol>
         *     <li>The subscription is owned by the provider user.</li>
         *     <li>The provided user has explicit permission to act as an owner of this subscription (typically reserved
         *         for administrators).</li>
         * </ol>
         */
        @Override
        public boolean canAccessSubscription(OwnedSubscription subscription) {
            return _ownerId.equals(subscription.getOwnerId()) ||
                    _internalAuthorizer.hasPermissionById(_ownerId,
                            Permissions.assumeDatabusSubscriptionOwnership(new NamedResource(subscription.getName())));
        }

        /**
         * A table can be polled by a user if that user has read permission on that table.
         */
        @Override
        public boolean canReceiveEventsFromTable(String table) {
            return _permissionCheckCache != null ?
                    _permissionCheckCache.getUnchecked(new OwnerTableCacheKey(_ownerId, table)) :
                    ownerCanReadTable(_ownerId, table);
        }
    }

    /**
     * Determines if an owner has read permission on a table.  This always calls back to the authorizer and will not
     * return a cached value.
     */
    private boolean ownerCanReadTable(String ownerId, String table) {
        return _internalAuthorizer.hasPermissionById(ownerId, getReadPermission(table));
    }

    /**
     * Gets the Permission instance for read permission on a table.  If caching is enabled the result is either returned
     * from or added to the cache.
     */
    private Permission getReadPermission(String table) {
        return _readPermissionCache != null ?
                _readPermissionCache.getUnchecked(table) :
                createReadPermission(table);
    }

    /**
     * Creates a Permission instance for read permission on a table.  This always resolves a new instance and will
     * not return a cached value.
     */
    private Permission createReadPermission(String table) {
        return _permissionResolver.resolvePermission(Permissions.readSorTable(new NamedResource(table)));
    }

    /**
     * Cache key for the permission cache by owner and table.
     */
    private static class OwnerTableCacheKey {
        private final String _ownerId;
        private final String _table;
        // Hash code is pre-computed and cached to reduce cache lookup time.
        private final int _hashCode;

        private OwnerTableCacheKey(String ownerId, String table) {
            _ownerId = ownerId;
            _table = table;
            _hashCode = Objects.hashCode(ownerId, table);
        }

        @Override
        public int hashCode() {
            return _hashCode;
        }

        @Override
        public boolean equals(Object o) {
            if (o == this) {
                return true;
            }
            if (!(o instanceof OwnerTableCacheKey)) {
                return false;
            }
            OwnerTableCacheKey that = (OwnerTableCacheKey) o;
            return _ownerId.equals(that._ownerId) && _table.equals(that._table);
        }
    }
}
