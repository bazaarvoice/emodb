package com.bazaarvoice.emodb.table.db.consistency;

import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.table.db.astyanax.FullConsistencyTimeProvider;
import com.google.common.base.Supplier;
import com.google.inject.Inject;

import java.time.Duration;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of {@link FullConsistencyTimeProvider} that sets the maximum full consistency timestamp
 */
public class HintsConsistencyTimeProvider implements FullConsistencyTimeProvider {
    public static final Duration DEFAULT_LAG = Duration.ofDays(1);

    private final Map<String, ValueStore<Long>> _timestampCache;

    @Inject
    public HintsConsistencyTimeProvider(@HintsConsistencyTimeValues Map<String, ValueStore<Long>> timestampCache) {
        _timestampCache = checkNotNull(timestampCache, "timestampCache");
    }

    @Override
    public long getMaxTimeStamp(String cluster) {
        Supplier<Long> holder = _timestampCache.get(cluster);
        Long timestamp = (holder != null) ? holder.get() : null;
        return (timestamp != null) ? timestamp : getDefaultTimestamp();
    }

    public static long getDefaultTimestamp() {
        return System.currentTimeMillis() - DEFAULT_LAG.toMillis();
    }
}
