package com.bazaarvoice.emodb.table.db.astyanax;

import com.bazaarvoice.emodb.sor.api.UnknownPlacementException;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Inject;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import java.util.Collection;
import java.util.List;

public class PlacementCache {
    private final PlacementFactory _placementFactory;
    private final LoadingCache<String, Placement> _placementCache;

    @Inject
    public PlacementCache(PlacementFactory placementFactory) {
        _placementFactory = placementFactory;

        // The placement cache maps placement strings to keyspace & column family settings.
        // The cardinality of the placement cache should be low, so don't bother expiring values.
        // Also, placement objects are used as keys in a few places, so not expiring provides == identity.
        _placementCache = CacheBuilder.newBuilder().build(new CacheLoader<String, Placement>() {
            @Override
            public Placement load(String placement) throws ConnectionException {
                return _placementFactory.newPlacement(placement);
            }
        });
    }

    public Placement get(String name) throws UnknownPlacementException {
        try {
            return _placementCache.getUnchecked(name);
        } catch (UncheckedExecutionException e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e.getCause());
        }
    }

    /** Returns all placements accessible in the local data center, including internal placements. */
    public Collection<String> getLocalPlacements() {
        List<String> placements = Lists.newArrayList();
        for (String placementName : _placementFactory.getValidPlacements()) {
            Placement placement;
            try {
                placement = get(placementName);
            } catch (UnknownPlacementException e) {
                continue;  // Placement must live in another data center.
            }
            placements.add(placement.getName());
        }
        return placements;
    }
}
