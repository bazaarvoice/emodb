package com.bazaarvoice.emodb.table.db.consistency;

import com.bazaarvoice.emodb.common.zookeeper.store.ValueStore;
import com.bazaarvoice.emodb.table.db.astyanax.FullConsistencyTimeProvider;
import com.google.common.base.Supplier;
import com.google.inject.Inject;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;


/**
 * Implementation of {@link FullConsistencyTimeProvider} that sets the maximum full consistency timestamp based
 * on a configured minimum lag time.  This lag time defines the range of TimeUUIDs accepted by the DataStore update
 * methods.  Any TimeUUID older than this lag will be rejected since otherwise it could be ignored by a compaction
 * performed after the TimeUUID.  Don't set this too small (eg. a few seconds) because clients should be allowed
 * to perform updates with a slightly old TimeUUID to allow for latency and retries w/in the client application.
 */
public class MinLagConsistencyTimeProvider implements FullConsistencyTimeProvider {
    public static final Duration DEFAULT_LAG = Duration.ofMinutes(5);

    private final Map<String, ValueStore<Duration>> _durationCache;

    @Inject
    public MinLagConsistencyTimeProvider(@MinLagDurationValues Map<String, ValueStore<Duration>> durationCache) {
        _durationCache = requireNonNull(durationCache, "durationCache");
    }

    @Override
    public long getMaxTimeStamp(String cluster) {
        // Convert the consistency lag duration to a millis timestamp.
        return Instant.now().minus(getConsistencyLag(cluster)).toEpochMilli();
    }

    private Duration getConsistencyLag(String cluster) {
        Supplier<Duration> holder = _durationCache.get(cluster);
        return Optional.ofNullable(holder != null ? holder.get() : null).orElse(DEFAULT_LAG);
    }
}
